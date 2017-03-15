import json
import logging
import urllib.parse

import requests

from autoscaler.config import Config
from autoscaler.kube import KubeResource

logger = logging.getLogger(__name__)


class ReservationClient(object):
    def _url(self, path):
        return urllib.parse.urljoin('http://reservation-api.{}/v1/'.format(Config.NAMESPACE), path)

    def list_reservations(self):
        req = requests.get(self._url('reservations'))
        req.raise_for_status()
        return req.json()


class Reservation(object):
    def __init__(self, data, instances, kube_nodes):
        self.id = data['id']
        self.username = data['username']
        self.name = data['name']
        self.node_selectors = json.loads(data['node_selectors'])

        self.resources = json.loads(data['resources'])

        self.instance_ids = set(inst_id for inst_id, inst in instances.items()
                                if getattr(inst, 'reservation_id', None) == self.id)
        self.nodes = [node for node in kube_nodes
                      if node.instance_id in self.instance_ids and
                      self._is_match(node)]

    def get_pending_resources(self):
        schedulable_nodes = [node for node in self.nodes
                             if not node.unschedulable]
        fulfilled = sum(
            (node.capacity for node in schedulable_nodes),
            KubeResource())

        return (self.kube_resources_requested - fulfilled,
                self.num_instances_requested - len(schedulable_nodes))

    @property
    def num_instances_requested(self):
        return self.resources.get('instances', 0)

    @property
    def kube_resources_requested(self):
        resources = dict(self.resources)
        resources.pop('instances', None)
        resources = KubeResource(**resources)
        return resources

    @property
    def tags(self):
        return {
            'openai.org/reservation-id': self.id
        }

    def _is_match(self, node):
        return all(node.selectors.get(k) == self.node_selectors[k]
                   for k in self.node_selectors.keys())

    def __str__(self):
        return 'Reservation({}, {})'.format(self.id, self.node_selectors)

    def __repr__(self):
        return str(self)
