import os


class Config(object):
    CAPACITY_DATA = os.environ.get('CAPACITY_DATA', 'data/capacity.json')
    CAPACITY_CPU_RESERVE = float(os.environ.get('CAPACITY_CPU_RESERVE', 0.0))

    NAMESPACE = os.environ.get('NAMESPACE', 'system')

    ENABLE_LEGACY_AZURE_CONTROLLER = os.environ.get('ENABLE_LEGACY_AZURE_CONTROLLER', False)
