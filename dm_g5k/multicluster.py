from dm_g5k.cluster import Cluster


class MultiCluster(Cluster):

    def __init__(self, clusters):
        self.clusters = clusters

    def initialize(self):
        for c in self.clusters:
            c.initialize()

    def bootstrap(self, dist_file):
        for c in self.clusters:
            c.bootstrap()

    def start(self):
        for c in self.clusters:
            c.start()

    def stop(self):
        for c in self.clusters:
            c.stop()

    def clean(self):
        for c in self.clusters:
            c.clean()