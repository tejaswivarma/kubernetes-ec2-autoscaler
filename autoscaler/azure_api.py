import logging
from copy import deepcopy
from datetime import datetime
from threading import RLock, Condition
from typing import List, Tuple, MutableMapping

import pytz
from abc import ABC
from azure.mgmt.compute import ComputeManagementClient
from azure.mgmt.compute.models import VirtualMachineScaleSet, Sku

from autoscaler.utils import Future

logger = logging.getLogger(__name__)


class AzureScaleSet:
    def __init__(self, location: str, resource_group: str, name: str, instance_type: str, capacity: int,
                 provisioning_state: str) -> None:
        self.name = name
        self.instance_type = instance_type
        self.capacity = capacity
        self.provisioning_state = provisioning_state
        self.resource_group = resource_group
        self.location = location

    def __str__(self):
        return 'AzureScaleSet({}, {}, {}, {})'.format(self.name, self.instance_type, self.capacity, self.provisioning_state)

    def __repr__(self):
        return str(self)

    def _key(self):
        return (self.name, self.instance_type, self.capacity, self.provisioning_state, self.resource_group, self.location)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, AzureScaleSet):
            return False
        return self._key() == other._key()

    def __hash__(self) -> int:
        return hash(self._key())


class AzureScaleSetInstance:
    def __init__(self, instance_id: str, vm_id: str, launch_time: datetime) -> None:
        self.instance_id = instance_id
        self.vm_id = vm_id
        self.launch_time = launch_time

    def __str__(self):
        return 'AzureScaleSetInstance({}, {}, {})'.format(self.instance_id, self.vm_id, self.launch_time)

    def __repr__(self):
        return str(self)

    def _key(self):
        return (self.instance_id, self.vm_id, self.launch_time)

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, AzureScaleSetInstance):
            return False
        return self._key() == other._key()

    def __hash__(self) -> int:
        return hash(self._key())


class AzureApi(ABC):
    def list_scale_sets(self, resource_group_name: str) -> List[AzureScaleSet]:
        pass

    def list_scale_set_instances(self, scale_set: AzureScaleSet) -> List[AzureScaleSetInstance]:
        pass

    def update_scale_set(self, scale_set: AzureScaleSet, new_capacity: int) -> Future:
        pass

    def terminate_scale_set_instances(self, scale_set: AzureScaleSet, instances: List[AzureScaleSetInstance]) -> Future:
        pass


class AzureWrapper(AzureApi):
    def __init__(self, client: ComputeManagementClient) -> None:
        self._client = client

    def list_scale_sets(self, resource_group_name: str) -> List[AzureScaleSet]:
        result = []
        for scale_set in self._client.virtual_machine_scale_sets.list(resource_group_name):
            result.append(AzureScaleSet(scale_set.location, resource_group_name, scale_set.name, scale_set.sku.name,
                                        scale_set.sku.capacity, scale_set.provisioning_state))
        return result

    def list_scale_set_instances(self, scale_set: AzureScaleSet) -> List[AzureScaleSetInstance]:
        result = []
        for instance in self._client.virtual_machine_scale_set_vms.list(scale_set.resource_group, scale_set.name, expand="instanceView"):
            launch_time = datetime.now(pytz.utc)
            for status in instance.instance_view.statuses:
                if status.code == 'ProvisioningState/succeeded':
                    launch_time = status.time
                    break
            result.append(AzureScaleSetInstance(instance.instance_id, instance.vm_id, launch_time))
        return result

    def update_scale_set(self, scale_set: AzureScaleSet, new_capacity: int) -> Future:
        parameters = VirtualMachineScaleSet(scale_set.location, sku=Sku(name=scale_set.instance_type, capacity=new_capacity))
        azure_op = self._client.virtual_machine_scale_sets.create_or_update(scale_set.resource_group, scale_set.name,
                                                                            parameters=parameters)
        return AzureOperationPollerFutureAdapter(azure_op)

    def terminate_scale_set_instances(self, scale_set: AzureScaleSet, instances: List[AzureScaleSetInstance]) -> Future:
        future = self._client.virtual_machine_scale_sets.delete_instances(scale_set.resource_group, scale_set.name, [instance.instance_id for instance in instances])
        return AzureOperationPollerFutureAdapter(future)


class AzureWriteThroughCachedApi(AzureApi):
    def __init__(self, delegate: AzureApi) -> None:
        self._delegate = delegate
        self._lock = RLock()
        self._instance_cache: MutableMapping[Tuple[str, str], List[AzureScaleSetInstance]] = {}
        self._scale_set_cache: MutableMapping[str, List[AzureScaleSet]] = {}

    def list_scale_sets(self, resource_group_name: str, force_refresh=False) -> List[AzureScaleSet]:
        if not force_refresh:
            with self._lock:
                if resource_group_name in self._scale_set_cache:
                    return deepcopy(self._scale_set_cache[resource_group_name])

        scale_sets = self._delegate.list_scale_sets(resource_group_name)
        with self._lock:
            old_scale_sets = dict((x.name, x) for x in self._scale_set_cache.get(resource_group_name, []))
            for scale_set in scale_sets:
                if scale_set.name not in old_scale_sets:
                    continue

                old_scale_set = old_scale_sets.get(scale_set.name)
                # Check if Scale Set was changed externally
                if old_scale_set.capacity != scale_set.capacity:
                    if (resource_group_name, scale_set.name) in self._instance_cache:
                        del self._instance_cache[(resource_group_name, scale_set.name)]

            self._scale_set_cache[resource_group_name] = scale_sets
        return deepcopy(scale_sets)

    def list_scale_set_instances(self, scale_set: AzureScaleSet) -> List[AzureScaleSetInstance]:
        key = (scale_set.resource_group, scale_set.name)
        with self._lock:
            if key in self._instance_cache:
                return deepcopy(self._instance_cache[key])

        instances = self._delegate.list_scale_set_instances(scale_set)
        with self._lock:
            self._instance_cache[key] = instances
        return deepcopy(instances)

    def update_scale_set(self, scale_set: AzureScaleSet, new_capacity: int) -> Future:
        future = self._delegate.update_scale_set(scale_set, new_capacity)
        future.add_done_callback(lambda _: self._invalidate(scale_set.resource_group, scale_set.name))
        return future

    def terminate_scale_set_instances(self, scale_set: AzureScaleSet, instances: List[AzureScaleSetInstance]) -> Future:
        future = self._delegate.terminate_scale_set_instances(scale_set, instances)
        future.add_done_callback(lambda _: self._invalidate(scale_set.resource_group, scale_set.name))
        return future

    def _invalidate(self, resource_group_name: str, scale_set_name: str) -> None:
        with self._lock:
            if (resource_group_name, scale_set_name) in self._instance_cache:
                del self._instance_cache[(resource_group_name, scale_set_name)]

            if resource_group_name in self._scale_set_cache:
                del self._scale_set_cache[resource_group_name]


# Adapts an Azure async operation to behave like a Future
class AzureOperationPollerFutureAdapter(Future):
    def __init__(self, azure_operation):
        self._result = None
        # NOTE: All this complexity with a Condition is here because AzureOperationPoller is not reentrant,
        # so a callback added with add_done_callback() could not call result(), if we delegated everything
        self._condition = Condition()
        self._callbacks = []
        azure_operation.add_done_callback(self._handle_completion)

    def _handle_completion(self, result):
        with self._condition:
            self._result = result
            self._condition.notifyAll()
            callbacks = self._callbacks
            self._callbacks.clear()

        for callback in callbacks:
            callback(self)

    def result(self):
        with self._condition:
            if self._result is None:
                self._condition.wait()
            return self._result

    def add_done_callback(self, fn):
        with self._condition:
            if self._result is not None:
                fn(self)
            else:
                self._callbacks.append(fn)