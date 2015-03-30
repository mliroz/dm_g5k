from abc import ABCMeta, abstractmethod


class Cluster(object):

    __metaclass__ = ABCMeta

    # Cluster state
    initialized = False
    running = False

    # General properties
    base_dir = None
    local_base_conf_dir = None

    # Nodes
    hosts = []
    master = None

    @abstractmethod
    def initialize(self):
        pass

    @abstractmethod
    def bootstrap(self, dist_file):
        pass

    @abstractmethod
    def start(self):
        pass

    @abstractmethod
    def stop(self):
        pass

    @abstractmethod
    def clean(self):
        pass