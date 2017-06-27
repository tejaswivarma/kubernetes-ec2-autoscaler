import unittest
from datetime import datetime, timedelta

import collections
import mock
import pytz
from azure.mgmt.compute.models import VirtualMachineScaleSetVM, \
    VirtualMachineInstanceView

from autoscaler.azure import AzureVirtualScaleSet
from autoscaler.azure_api import AzureScaleSet, AzureWrapper


class TestCluster(unittest.TestCase):
    def test_scale_up(self):
        region = 'test'
        mock_client = mock.Mock()
        mock_client.virtual_machine_scale_set_vms = mock.Mock()
        mock_client.virtual_machine_scale_set_vms.list = mock.Mock(return_value=[])
        mock_client.virtual_machine_scale_sets = mock.Mock()
        mock_client.virtual_machine_scale_sets.create_or_update = mock.Mock()

        monitor_client = mock.Mock()
        monitor_client.activity_logs = mock.Mock()
        monitor_client.activity_logs.list = mock.Mock(return_value=[])

        instance_type = 'Standard_D1_v2'
        resource_group = 'test-resource-group'
        scale_set = AzureScaleSet(region, resource_group, 'test-scale-set', instance_type, 0, 'Succeeded')

        virtual_scale_set = AzureVirtualScaleSet(region, resource_group, AzureWrapper(mock_client, monitor_client), instance_type, [scale_set], [])

        virtual_scale_set.scale(5)

        mock_client.virtual_machine_scale_sets.create_or_update.assert_called_once()
        self.assertEqual(mock_client.virtual_machine_scale_sets.create_or_update.call_args[1]['parameters'].sku.capacity, 5)

    def test_out_of_quota(self):
        region = 'test'
        mock_client = mock.Mock()
        mock_client.virtual_machine_scale_set_vms = mock.Mock()
        mock_client.virtual_machine_scale_set_vms.list = mock.Mock(return_value=[])
        mock_client.virtual_machine_scale_sets = mock.Mock()
        mock_client.virtual_machine_scale_sets.create_or_update = mock.Mock()

        monitor_client = mock.Mock()
        monitor_client.activity_logs = mock.Mock()
        monitor_client.activity_logs.list = mock.Mock(return_value=[])

        instance_type = 'Standard_D1_v2'
        resource_group = 'test-resource-group'
        scale_set = AzureScaleSet(region, resource_group, 'test-scale-set', instance_type, 0, 'Succeeded',
                                  timeout_until=datetime.now(pytz.utc) + timedelta(minutes=10), timeout_reason="fake reason")
        virtual_scale_set = AzureVirtualScaleSet(region, resource_group, AzureWrapper(mock_client, monitor_client), instance_type, [scale_set], [])
        self.assertTrue(virtual_scale_set.is_timed_out())

    def test_scale_in(self):
        region = 'test'
        resource_group = 'test-resource-group'

        instance = VirtualMachineScaleSetVM(location=region)
        instance.vm_id = 'test-vm-id'
        instance.instance_id = 0
        instance.instance_view = VirtualMachineInstanceView()
        instance.instance_view.statuses = []

        mock_client = mock.Mock()
        mock_client.virtual_machine_scale_set_vms = mock.Mock()
        mock_client.virtual_machine_scale_set_vms.list = mock.Mock(return_value=[instance])
        mock_client.virtual_machine_scale_sets = mock.Mock()
        mock_client.virtual_machine_scale_sets.delete_instances = mock.Mock()

        monitor_client = mock.Mock()
        monitor_client.activity_logs = mock.Mock()
        monitor_client.activity_logs.list = mock.Mock(return_value=[])

        TestNode = collections.namedtuple('TestNode', ['instance_id', 'unschedulable'])
        test_node = TestNode(instance_id=instance.vm_id, unschedulable=False)

        instance_type = 'Standard_D1_v2'
        scale_set = AzureScaleSet(region, resource_group, 'test-scale-set', instance_type, 1, 'Succeeded')

        virtual_scale_set = AzureVirtualScaleSet(region, resource_group, AzureWrapper(mock_client, monitor_client), instance_type, [scale_set], [test_node])

        self.assertEqual(virtual_scale_set.instance_ids, {instance.vm_id})
        self.assertEqual(virtual_scale_set.nodes, [test_node])

        virtual_scale_set.scale_nodes_in([test_node])
        mock_client.virtual_machine_scale_sets.delete_instances.assert_called_once_with(resource_group, scale_set.name, [instance.instance_id])
