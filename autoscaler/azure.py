import json
import logging
import urlparse

import requests

from autoscaler.autoscaling_groups import AutoScalingGroup
import autoscaler.utils as utils

logger = logging.getLogger(__name__)


class AzureClient(object):
    def __init__(self, region='us-south-central'):
        self.region = region

    def _url(self, path):
        return urlparse.urljoin('http://azure-{}.system/'.format(self.region), path)

    def list_instances(self):
        req = requests.get(self._url('instances'))
        req.raise_for_status()
        return req.json()

    def create_instances(self, instance_type, number, tags):
        data = {
            'instance_type': instance_type,
            'number': number,
            'tags': json.dumps(tags)
        }
        req = requests.post(self._url('instances'), data=data)
        req.raise_for_status()
        return req.json()

    def delete_instances(self, instance_id):
        req = requests.delete(self._url('instances/{}'.format(instance_id)))
        req.raise_for_status()
        return req.json()

    def get_tags(self):
        req = requests.get(self._url('available-tags'))
        req.raise_for_status()
        return req.json()


class AzureGroups(object):
    def __init__(self, regions):
        self.regions = regions

    def get_all_groups(self, kube_nodes):
        groups = []
        for region in self.regions:
            client = AzureClient(region)

            tags = client.get_tags()
            instances = client.list_instances()

            for tag_set in tags['allowed-tag-sets']:
                instance_type = tag_set['instance_type']
                tags = tag_set['tags']
                group_instances = [
                    inst for inst in instances
                    if inst['instance_type'] == instance_type and all(inst['tags'][k] == tags[k] for k in tags.keys())]
                group = AzureGroup(instance_type, tags, group_instances, kube_nodes)
                groups.append(group)

        return groups


class AzureGroup(AutoScalingGroup):
    def __init__(self, client, instance_type, tags, instances, kube_nodes):
        self.client = client
        self.instance_type = instance_type
        self.selectors = tags
        self.name = instance_type
        self.desired_capacity = len(instances)
        self.min_size = 0
        self.max_size = 1000

        self.instance_ids = set(inst['id'] for inst in instances)
        self.nodes = [node for node in kube_nodes
                      if node.instance_id in self.instance_ids]
        self.unschedulable_nodes = filter(
            lambda n: n.unschedulable, self.nodes)

        self._id = (client.region, self.name)

    def set_desired_capacity(self, new_desired_capacity):
        """
        sets the desired capacity of the underlying ASG directly.
        note that this is for internal control.
        for scaling purposes, please use scale() instead.
        """
        logger.info("ASG: {} new_desired_capacity: {}".format(
            self, new_desired_capacity))
        self.client.create_instances(self.instance_type,
                                     new_desired_capacity - len(self.instance_ids),
                                     self.selectors)
        self.desired_capacity = new_desired_capacity

    def scale_node_in(self, node):
        """
        scale down asg by terminating the given node.
        returns True if node was successfully terminated.
        """
        self.client.delete_instances(node.instance_id)
        self.nodes.remove(node)
        logger.info('Scaled node %s in', node)
        return True

    def __str__(self):
        return 'AzureGroup({name}, {selectors_hash})'.format(name=self.name, selectors_hash=utils.selectors_to_hash(self.selectors))

    def __repr__(self):
        return str(self)
