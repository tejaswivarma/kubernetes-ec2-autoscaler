import json
import logging
import urlparse

import requests

from autoscaler.config import Config
from autoscaler.kube import KubeResource

logger = logging.getLogger(__name__)


class ReservationClient(object):
    def _url(self, path):
        return urlparse.urljoin('http://reservation-api.{}/v1/'.format(Config.NAMESPACE), path)

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
                      if node.instance_id in self.instance_ids]

    def get_pending_resources(self):
        fulfilled = sum((node.capacity for node in self.nodes), KubeResource())

        num_instances_requested = self.resources.get('instances', 0)

        resources = dict(self.resources)
        resources.pop('instances', None)
        resources = KubeResource(**resources)

        logger.debug('Reservation %s: resources[%s] instances [%s]', self, resources, num_instances_requested)

        return ((resources - fulfilled), num_instances_requested - len(self.nodes))

    @property
    def tags(self):
        return {
            'openai.org/reservation-id': self.id
        }

    def __str__(self):
        return 'Reservation({}, {})'.format(self.id, self.node_selectors)

    def __repr__(self):
        return str(self)
