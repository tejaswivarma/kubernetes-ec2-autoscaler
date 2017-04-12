import http
import logging
import urllib.parse
import re
from datetime import datetime
from threading import Condition

from azure.mgmt.compute.models import Sku
from azure.mgmt.compute.models import VirtualMachineScaleSet
from azure.mgmt.compute import ComputeManagementClient
from dateutil.parser import parse as dateutil_parse
import requests
import requests.exceptions
import pytz
from msrest.pipeline import ClientRetry

from autoscaler.config import Config
from autoscaler.autoscaling_groups import AutoScalingGroup
from autoscaler.utils import TransformingFuture, AllCompletedFuture, CompletedFuture
import autoscaler.errors as errors
import autoscaler.utils as utils

logger = logging.getLogger(__name__)

_DEFAULT_TAG_VALUE = 'default'
UNRESERVED_HOST = 'legacy-default-reservation-do-not-use'


# Adapts an Azure async operation to behave like a Future
class AzureOperationPollerFutureAdapter:
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


_RETRY_TIME_LIMIT = 30


class AzureBoundedRetry(ClientRetry):
    """
    XXX: Azure sometimes sends us a Retry-After: 1200, even when we still have quota, causing our client to appear to hang.
    Ignore them and just retry after 30secs
    """
    def __init__(self, **kwargs):
        super().__init__(**kwargs)

    @staticmethod
    def from_retry(retry):
        new_retry = AzureBoundedRetry()
        new_retry.total = retry.total
        new_retry.connect = retry.connect
        new_retry.read = retry.read
        new_retry.backoff_factor = retry.backoff_factor
        new_retry.BACKOFF_MAX = retry.BACKOFF_MAX
        new_retry.status_forcelist = retry.status_forcelist
        new_retry.method_whitelist = retry.method_whitelist

        return new_retry

    def get_retry_after(self, response):
        retry_after = super().get_retry_after(response)
        if response.status != http.HTTPStatus.TOO_MANY_REQUESTS or retry_after <= _RETRY_TIME_LIMIT:
            return retry_after

        headers = {}
        for header in ['Retry-After',
                       'x-ms-ratelimit-remaining-subscription-reads',
                       'x-ms-ratelimit-remaining-subscription-writes',
                       'x-ms-ratelimit-remaining-tenant-reads',
                       'x-ms-ratelimit-remaining-tenant-writes',
                       'x-ms-ratelimit-remaining-subscription-resource-requests',
                       'x-ms-ratelimit-remaining-subscription-resource-entities-read',
                       'x-ms-ratelimit-remaining-tenant-resource-requests',
                       'x-ms-ratelimit-remaining-tenant-resource-entities-read']:
            value = response.getheader(header)
            if value is not None:
                headers[header] = value

        logger.warn("Azure request throttled: {}".format(headers))
        return _RETRY_TIME_LIMIT


class AzureClient(object):
    def __init__(self, region='us-south-central'):
        self.region = region

    def _url(self, path):
        return urllib.parse.urljoin('http://azure-{}.{}/'.format(self.region, Config.NAMESPACE), path)

    def list_instances(self):
        try:
            req = requests.get(self._url('instances'))
            req.raise_for_status()
        except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError) as e:
            errors.capture_exception()
            return {
                'error': str(e)
            }

        return req.json()

    def create_instances(self, instance_type, number, tags):
        url = self._url('instances')
        data = {
            'instance_type': instance_type,
            'number': number,
            'tags': tags
        }

        logger.debug('POST %s (data=%s)', url, data)

        try:
            req = requests.post(url, json=data)

            logger.debug('response: %s', req.text)

            req.raise_for_status()
        except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError) as e:
            errors.capture_exception()
            return {
                'error': str(e)
            }

        return req.json()

    def delete_instances(self, instance_id):
        try:
            req = requests.delete(self._url('instances/{}'.format(instance_id)))
            req.raise_for_status()
        except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError) as e:
            errors.capture_exception()
            return {
                'error': str(e)
            }

        return req.json()

    def get_tags(self):
        try:
            req = requests.get(self._url('allowed_launch_parameters'))
            req.raise_for_status()
        except (requests.exceptions.HTTPError, requests.exceptions.ConnectionError) as e:
            errors.capture_exception()
            return {
                'error': str(e)
            }
        return req.json()


class AzureGroups(object):
    def __init__(self, legacy_regions, resource_groups, credentials, subscription_id):
        self.legacy_regions = legacy_regions
        self.resource_groups = resource_groups
        self.credentials = credentials
        self.subscription_id = subscription_id

    def get_all_groups(self, kube_nodes):

        groups = []
        for region in self.legacy_regions:
            client = AzureClient(region)

            tags = client.get_tags()
            instances = client.list_instances()

            if 'error' in instances or 'error' in tags:
                logger.warn('Failed to get instances in %s. Skipping.', region)
                continue

            for tag_set in tags['parameter_sets']:
                instance_type = tag_set['instance_type']
                tags = tag_set['tags']
                group_instances = [
                    AzureInstance(inst['id'], inst['instance_type'], dateutil_parse(inst['launch_time']), inst['tags']) for inst in instances['instances']
                    if inst['instance_type'] == instance_type and
                    all(inst['tags'].get(k) == tags[k] for k in tags.keys())]
                for tag in tag_set.get('arbitrary_value_tags', []):
                    tags[tag] = _DEFAULT_TAG_VALUE
                group = AzureGroup(client, instance_type, tags, group_instances, kube_nodes)
                groups.append(group)
        if self.credentials:
            compute_client = ComputeManagementClient(self.credentials, self.subscription_id)
            compute_client.config.retry_policy.policy = AzureBoundedRetry.from_retry(compute_client.config.retry_policy.policy)
            for resource_group in self.resource_groups:
                scale_sets_by_type = {}
                for scale_set in compute_client.virtual_machine_scale_sets.list(resource_group):
                    if scale_set.provisioning_state == 'Failed':
                        logger.error("{} failed provisioning. Ignoring it.".format(scale_set.name))
                        continue
                    scale_sets_by_type.setdefault((scale_set.location, scale_set.sku.name), []).append(scale_set)
                for key, scale_sets in scale_sets_by_type.items():
                    location, instance_type = key
                    groups.append(AzureVirtualScaleSet(location, resource_group, compute_client, instance_type, scale_sets, kube_nodes))

        return groups


_CLASS_PAT = re.compile(r'\w+_(?P<class>[A-Z]+).+')


def _get_azure_class(type_):
    m = _CLASS_PAT.match(type_)
    return m.group('class')


_SCALE_SET_SIZE_LIMIT = 40


# Appears as an unbounded scale set. Currently, Azure Scale Sets have a limit of 100 hosts.
class AzureVirtualScaleSet(AutoScalingGroup):
    provider = 'azure'

    def __init__(self, region, resource_group, client, instance_type, scale_sets, kube_nodes):
        self.client = client
        self.instance_type = instance_type
        # TODO: Remove this. Legacy value indicating that this node is optimized for compute
        self.tags = {'openai/computing': 'true'}
        self.name = 'virtual_scale_set_' + instance_type + '_' + region + '_' + resource_group
        self.scale_sets = dict((scale_set.name, scale_set) for scale_set in scale_sets)
        self.desired_capacity = sum(scale_set.sku.capacity for scale_set in scale_sets)

        self.region = region
        self.resource_group = resource_group

        self.selectors = dict(self.tags)
        # HACK: for matching node selectors
        self.selectors['azure/type'] = self.instance_type
        self.selectors['azure/region'] = self.region
        self.selectors['azure/class'] = _get_azure_class(self.instance_type)

        self.min_size = 0
        self.max_size = 1000
        self.is_spot = False

        self.vm_to_instance_id = {}
        self.instances = {}
        for scale_set in scale_sets:
            for instance in self.client.virtual_machine_scale_set_vms.list(self.resource_group, scale_set.name, expand="instanceView"):
                self.vm_to_instance_id[instance.vm_id] = (scale_set.name, instance.instance_id)
                launch_time = datetime.now(pytz.utc)
                for status in instance.instance_view.statuses:
                    if status.code == 'ProvisioningState/succeeded':
                        launch_time = status.time
                        break
                self.instances[instance.vm_id] = AzureInstance(instance.vm_id, self.instance_type, launch_time, self.tags)

        self.nodes = [node for node in kube_nodes if node.instance_id in self.vm_to_instance_id]
        self.unschedulable_nodes = [n for n in self.nodes if n.unschedulable]

        self._id = (self.region, self.name)

    def get_azure_instances(self):
        return self.instances.values()

    @property
    def instance_ids(self):
        return self.vm_to_instance_id.keys()

    def set_desired_capacity(self, new_desired_capacity):
        """
        sets the desired capacity of the underlying ASG directly.
        note that this is for internal control.
        for scaling purposes, please use scale() instead.
        """
        scale_out = new_desired_capacity - self.desired_capacity
        assert scale_out >= 0
        if scale_out == 0:
            return CompletedFuture(False)

        futures = []
        for scale_set in self.scale_sets.values():
            if scale_set.sku.capacity < _SCALE_SET_SIZE_LIMIT:
                new_group_capacity = min(_SCALE_SET_SIZE_LIMIT, scale_set.sku.capacity + scale_out)
                scale_out -= (new_group_capacity - scale_set.sku.capacity)
                # Update our cached version
                self.scale_sets[scale_set.name].sku.capacity = new_group_capacity
                if scale_set.provisioning_state == 'Updating':
                    logger.warn("Update of {} already in progress".format(scale_set.name))
                    continue
                parameters = VirtualMachineScaleSet(self.region, sku=Sku(name=self.instance_type, capacity=new_group_capacity))
                azure_op = self.client.virtual_machine_scale_sets.create_or_update(self.resource_group, scale_set.name,
                                                                                   parameters=parameters)
                futures.append(AzureOperationPollerFutureAdapter(azure_op))
                logger.info("Scaling Azure Scale Set {} to {}".format(scale_set.name, new_group_capacity))
            if scale_out == 0:
                break

        if scale_out > 0:
            logger.error("Not enough scale sets to reach desired capacity {} for {}".format(new_desired_capacity, self))

        self.desired_capacity = new_desired_capacity - scale_out
        logger.info("ASG: {} new_desired_capacity: {}".format(self, new_desired_capacity))

        return TransformingFuture(True, AllCompletedFuture(futures))

    def terminate_instances(self, vm_ids):
        instance_ids = {}
        for vm_id in vm_ids:
            scale_set_name, instance_id = self.vm_to_instance_id[vm_id]
            # Update our cached copy of the Scale Set
            self.scale_sets[scale_set_name].sku.capacity -= 1
            instance_ids.setdefault(scale_set_name, []).append(instance_id)
        logger.info('Terminated instances %s', list(vm_ids))

        futures = []
        for scale_set_name, ids in instance_ids.items():
            future = self.client.virtual_machine_scale_sets.delete_instances(self.resource_group, scale_set_name, ids)
            futures.append(AzureOperationPollerFutureAdapter(future))
        return AllCompletedFuture(futures)

    def scale_nodes_in(self, nodes):
        """
        scale down asg by terminating the given node.
        returns a future indicating when the request completes.
        """
        for node in nodes:
            self.nodes.remove(node)
        return self.terminate_instances(node.instance_id for node in nodes)

    def __str__(self):
        return 'AzureVirtualScaleSet({name}, {selectors_hash})'.format(name=self.name, selectors_hash=utils.selectors_to_hash(self.selectors))

    def __repr__(self):
        return str(self)

class AzureGroup(AutoScalingGroup):
    provider = 'azure'

    def __init__(self, client, instance_type, tags, instances, kube_nodes):
        self.client = client
        self.instance_type = instance_type
        self.tags = tags
        # XXX: backwards compatibility hack, for when reservations were implemented with Azure tags
        self.tags['openai.org/reservation-id'] = UNRESERVED_HOST
        self.name = instance_type
        self.desired_capacity = len(instances)

        self.region = client.region

        self.selectors = dict(tags)
        # HACK: for matching node selectors
        self.selectors['azure/type'] = self.instance_type
        self.selectors['azure/region'] = self.region
        self.selectors['azure/class'] = _get_azure_class(self.instance_type)

        self.min_size = 0
        self.max_size = 1000
        self.is_spot = False

        self.instances = dict((inst.id, inst) for inst in instances)
        self.nodes = [node for node in kube_nodes
                      if node.instance_id in self.instances]
        self.unschedulable_nodes = [n for n in self.nodes if n.unschedulable]

        self._id = (self.region, self.name)

    @property
    def instance_ids(self):
        return set(self.instances.keys())

    def set_desired_capacity(self, new_desired_capacity):
        """
        sets the desired capacity of the underlying ASG directly.
        note that this is for internal control.
        for scaling purposes, please use scale() instead.
        """
        logger.info("ASG: {} new_desired_capacity: {}".format(
            self, new_desired_capacity))

        self.client.create_instances(self.instance_type,
                                     new_desired_capacity - len(self.instances),
                                     self.tags)
        self.desired_capacity = new_desired_capacity
        return CompletedFuture(True)

    def terminate_instances(self, instance_ids):
        for instance_id in instance_ids:
            self.client.delete_instances(instance_id)
        logger.info('Terminated instances %s', list(instance_ids))
        return CompletedFuture(None)

    def scale_nodes_in(self, nodes):
        """
        scale down asg by terminating the given node.
        returns a future indicating when the request completes.
        """
        self.terminate_instances(node.instance_id for node in nodes)
        for node in nodes:
            self.nodes.remove(node)
        return CompletedFuture(None)

    def __str__(self):
        return 'AzureGroup({name}, {selectors_hash})'.format(name=self.name, selectors_hash=utils.selectors_to_hash(self.selectors))

    def __repr__(self):
        return str(self)


class AzureInstance(object):
    provider = 'azure'

    def __init__(self, instance_id, instance_type, launch_time, tags):
        self.id = instance_id
        self.instance_type = instance_type
        self.launch_time = launch_time
        self.tags = tags

    def __str__(self):
        return 'AzureInstance({}, {})'.format(self.id, self.instance_type)

    def __repr__(self):
        return str(self)
