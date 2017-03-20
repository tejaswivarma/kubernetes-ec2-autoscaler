import json
import logging
import urllib.parse

import requests

from autoscaler.config import Config
from autoscaler.kube import KubeResource

logger = logging.getLogger(__name__)

DEFAULT_ID = 'default'


class ReservationClient(object):
    def _url(self, path):
        return urllib.parse.urljoin('http://reservation-api.{}/v1/'.format(Config.NAMESPACE), path)

    def list_reservations(self):
        req = requests.get(self._url('reservations'))
        req.raise_for_status()
        return req.json()


class Reservation(object):
    def __init__(self, data, kube_nodes):
        self.id = data['id']
        self.username = data['username']
        self.name = data['name']
        self.node_selectors = json.loads(data['node_selectors'])

        self.resources = json.loads(data['resources'])

        self.nodes = [node for node in kube_nodes
                      if node.reservation_id == self.id and
                      self.is_match(node)]

    def get_pending_resources(self):
        schedulable_nodes = [node for node in self.nodes
                             if not node.unschedulable]
        fulfilled = sum(
            (node.capacity for node in schedulable_nodes),
            KubeResource())

        return (self.kube_resources_requested - fulfilled,
                self.num_instances_requested - len(schedulable_nodes))

    def add_node(self, node):
        assert node.reservation_id == self.id
        self.nodes.append(node)

    @property
    def num_instances_requested(self):
        return self.resources.get('instances', 0)

    @property
    def kube_resources_requested(self):
        resources = dict(self.resources)
        resources.pop('instances', None)
        resources = KubeResource(**resources)
        return resources

    def is_match(self, node):
        return all(node.selectors.get(k) == self.node_selectors[k]
                   for k in self.node_selectors.keys())

    def __str__(self):
        return 'Reservation({}, {})'.format(self.id, self.node_selectors)

    def __repr__(self):
        return str(self)
