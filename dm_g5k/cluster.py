from abc import ABCMeta, abstractmethod


class Cluster(object):

    __metaclass__ = ABCMeta

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