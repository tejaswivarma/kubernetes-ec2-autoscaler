import logging
import urllib.parse
import re

from dateutil.parser import parse as dateutil_parse
import requests
import requests.exceptions

from autoscaler.config import Config
from autoscaler.autoscaling_groups import AutoScalingGroup
import autoscaler.errors as errors
import autoscaler.utils as utils

logger = logging.getLogger(__name__)

_DEFAULT_TAG_VALUE = 'default'
UNRESERVED_HOST = 'legacy-default-reservation-do-not-use'

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
    def __init__(self, regions):
        self.regions = regions

    def get_all_groups(self, kube_nodes):
        groups = []
        for region in self.regions:
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
                    AzureInstance(inst) for inst in instances['instances']
                    if inst['instance_type'] == instance_type and
                    all(inst['tags'].get(k) == tags[k] for k in tags.keys())]
                for tag in tag_set.get('arbitrary_value_tags', []):
                    tags[tag] = _DEFAULT_TAG_VALUE
                group = AzureGroup(client, instance_type, tags, group_instances, kube_nodes)
                groups.append(group)

        return groups


_CLASS_PAT = re.compile(r'\w+_(?P<class>[A-Z]+).+')


def _get_azure_class(type_):
    m = _CLASS_PAT.match(type_)
    return m.group('class')


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

    def terminate_instance(self, instance_id):
        self.client.delete_instances(instance_id)
        logger.info('Terminated instance %s', instance_id)
        return True

    def scale_node_in(self, node):
        """
        scale down asg by terminating the given node.
        returns True if node was successfully terminated.
        """
        self.terminate_instance(node.instance_id)
        self.nodes.remove(node)
        return True

    def __str__(self):
        return 'AzureGroup({name}, {selectors_hash})'.format(name=self.name, selectors_hash=utils.selectors_to_hash(self.selectors))

    def __repr__(self):
        return str(self)


class AzureInstance(object):
    provider = 'azure'

    def __init__(self, data):
        self.id = data['id']
        self.instance_type = data['instance_type']
        self.launch_time = dateutil_parse(data['launch_time'])
        self.tags = data['tags']

    def __str__(self):
        return 'AzureInstance({}, {})'.format(self.id, self.instance_type)

    def __repr__(self):
        return str(self)
