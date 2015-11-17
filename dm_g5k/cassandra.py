import os
import shutil
import tempfile
import yaml

from ConfigParser import ConfigParser
from execo import SshProcess

from execo.action import TaktukPut, Get, Remote, TaktukRemote, \
    SequentialActions
from execo_engine import logger
from execo_g5k.api_utils import get_host_cluster
from subprocess import call
from dm_g5k.cluster import Cluster, ClusterNotInitializedException

# Configuration files
CONF_FILE = "cassandra.yaml"

# Default parameters
DEFAULT_CASSANDRA_BASE_DIR = "/tmp/cassandra"
DEFAULT_CASSANDRA_CONF_DIR = DEFAULT_CASSANDRA_BASE_DIR + "/conf"
DEFAULT_CASSANDRA_LOGS_DIR = DEFAULT_CASSANDRA_BASE_DIR + "/logs"

DEFAULT_CASSANDRA_LOCAL_CONF_DIR = "conf"


class CassandraCluster(Cluster):
    """This class manages the whole life-cycle of a Cassandra cluster.

    """

    # Cluster state
    initialized = False
    running = False
    running_cassandra = False

    # Default properties
    defaults = {
        "cassandra_base_dir": DEFAULT_CASSANDRA_BASE_DIR,
        "cassandra_conf_dir": DEFAULT_CASSANDRA_CONF_DIR,
        "cassandra_logs_dir": DEFAULT_CASSANDRA_LOGS_DIR,

        "local_base_conf_dir": DEFAULT_CASSANDRA_LOCAL_CONF_DIR
    }

    @staticmethod
    def get_cluster_type():
        return "cassandra"

    def __init__(self, hosts, config_file=None):
        """Create a new Cassandra cluster with the given hosts.

        Args:
          hosts (list of Host):
            The hosts that conform the cluster.
          config_file (str, optional):
            The path of the config file to be used.
        """

        # Load cluster properties
        config = ConfigParser(self.defaults)
        config.add_section("cluster")
        config.add_section("local")

        if config_file:
            config.readfp(open(config_file))

        self.base_dir = config.get("cluster", "cassandra_base_dir")
        self.conf_dir = config.get("cluster", "cassandra_conf_dir")
        self.logs_dir = config.get("cluster", "cassandra_logs_dir")
        self.local_base_conf_dir = config.get("local", "local_base_conf_dir")

        self.bin_dir = self.base_dir + "/bin"

        # Configure nodes and seeds
        self.hosts = hosts
        self.seeds = self.hosts[0:3]

        # TODO: Temporary
        self.master = self.hosts[0]

        # Store cluster information
        self.host_clusters = {}
        for h in self.hosts:
            g5k_cluster = get_host_cluster(h)
            if g5k_cluster in self.host_clusters:
                self.host_clusters[g5k_cluster].append(h)
            else:
                self.host_clusters[g5k_cluster] = [h]

        logger.info("Cassandra cluster created with hosts " + str(self.hosts))

    def bootstrap(self, tar_file):
        """Install Cassandra in all cluster nodes from the specified tar.gz file.

        Args:
          tar_file (str):
            The file containing Cassandra binaries.
        """

        # 0. Check that required packages are present
        required_packages = "openjdk-7-jre openjdk-7-jdk"
        check_packages = TaktukRemote("dpkg -s " + required_packages,
                                      self.hosts)
        for p in check_packages.processes:
            p.nolog_exit_code = p.nolog_error = True
        check_packages.run()
        if not check_packages.ok:
            logger.info("Packages not installed, trying to install")
            install_packages = TaktukRemote(
                "export DEBIAN_MASTER=noninteractive ; " +
                "apt-get update && apt-get install -y --force-yes " +
                required_packages, self.hosts).run()
            if not install_packages.ok:
                logger.error("Unable to install the packages")

        get_java_home = SshProcess('echo $(readlink -f /usr/bin/javac | '
                               'sed "s:/bin/javac::")', self.master)
        get_java_home.run()
        self.java_home = get_java_home.stdout.strip()

        logger.info("All required packages are present")

        # 1. Copy hadoop tar file and uncompress
        logger.info("Copy " + tar_file + " to hosts and uncompress")
        rm_dirs = TaktukRemote("rm -rf " + self.base_dir +
                               " " + self.conf_dir +
                               " " + self.logs_dir,
                                self.hosts)
        put_tar = TaktukPut(self.hosts, [tar_file], "/tmp")
        tar_xf = TaktukRemote(
            "tar xf /tmp/" + os.path.basename(tar_file) + " -C /tmp",
            self.hosts)
        SequentialActions([rm_dirs, put_tar, tar_xf]).run()

        # 2. Move installation to base dir and create other dirs
        logger.info("Create installation directories")
        mv_base_dir = TaktukRemote(
            "mv /tmp/" +
            os.path.basename(tar_file).replace(".tar.gz", "") + " " +
            self.base_dir,
            self.hosts)
        mkdirs = TaktukRemote("mkdir -p " + self.conf_dir +
                              " && mkdir -p " + self.logs_dir,
                              self.hosts)
        chmods = TaktukRemote("chmod g+w " + self.base_dir +
                              " && chmod g+w " + self.conf_dir +
                              " && chmod g+w " + self.logs_dir,
                              self.hosts)
        SequentialActions([mv_base_dir, mkdirs, chmods]).run()

    def initialize(self):
        """Initialize the cluster: copy base configuration and format DFS."""

        self._pre_initialize()

        logger.info("Initializing Cassandra")

        # Set basic configuration
        self._copy_base_conf()
        self._create_nodes_and_seeds_conf()

        # Configure hosts depending on resource type
        for g5k_cluster in self.host_clusters:
            hosts = self.host_clusters[g5k_cluster]
            self._configure_servers(hosts)
            self._copy_conf(self.conf_dir, hosts)

        self.initialized = True

    def _pre_initialize(self):
        """Clean previous configurations"""

        if self.initialized:
            if self.running:
                self.stop()
            self.clean()
        else:
            self.__force_clean()

        self.initialized = False

    def _copy_base_conf(self):
        """Copy base configuration files to tmp dir."""

        self.temp_conf_dir = tempfile.mkdtemp("", "cassandra-", "/tmp")
        if os.path.exists(self.local_base_conf_dir):
            base_conf_files = [os.path.join(self.local_base_conf_dir, f)
                               for f in os.listdir(self.local_base_conf_dir)]
            for f in base_conf_files:
                shutil.copy(f, self.temp_conf_dir)
        else:
            logger.warn(
                "Local conf dir does not exist. Using default configuration")
            base_conf_files = []

        mandatory_files = [CONF_FILE]

        missing_conf_files = mandatory_files
        for f in base_conf_files:
            f_base_name = os.path.basename(f)
            if f_base_name in missing_conf_files:
                missing_conf_files.remove(f_base_name)

        logger.info("Copying missing conf files from master: " + str(
            missing_conf_files))

        remote_missing_files = [os.path.join(self.conf_dir, f)
                                for f in missing_conf_files]

        action = Get([self.hosts[0]], remote_missing_files, self.temp_conf_dir)
        action.run()

    def _create_nodes_and_seeds_conf(self):
        """Create master and slaves configuration files."""

        with open(os.path.join(self.conf_dir, CONF_FILE)) as stream:
            config = yaml.load(stream)

        print config

        # Change configuration
        config["seed_provider"][0]["parameters"][0]["seeds"] = \
            '"' + ",".join(s.address for s in self.seeds) + '"'

        with open(os.path.join(self.conf_dir, CONF_FILE), "w") as stream:
            yaml.dump(config, stream)

    def _check_initialization(self):
        """ Check whether the cluster is initialized and raise and exception if
        not.
        """

        if not self.initialized:
            logger.error("The cluster should be initialized")
            raise ClusterNotInitializedException(
                "The cluster should be initialized")

    def _configure_servers(self, hosts=None):
        pass

    def _copy_conf(self, conf_dir, hosts=None):

        if not hosts:
            hosts = self.hosts

        conf_files = [os.path.join(conf_dir, f) for f in os.listdir(conf_dir)]

        action = TaktukPut(hosts, conf_files, self.conf_dir)

        action.run()

        if not action.finished_ok:
            logger.warn("Error while copying configuration")
            if not action.ended:
                action.kill()

    def start(self):

        self._check_initialization()

        logger.info("Starting Cassandra")

        if self.running_cassandra:
            logger.warn("Cassandra was already started")
            return

        proc = TaktukRemote(self.bin_dir + "/cassandra", self.hosts)
        proc.run()

        if not proc.finished_ok:
            logger.warn("Error while starting Cassandra")
        else:
            self.running_cassandra = True
            self.running = True

    def start_shell(self, node=None, exec_params=None):
        """Open a Hive shell.

        Args:
          node (Host, optional):
            The host were the shell is to be started. If not provided,
            self.master is chosen.
          exec_params (str, optional):
            The list of parameters used in job execution.
        """

        if not node:
            node = self.master

        # Configure execution options
        if not exec_params:
            exec_params = []

        params_str = " " + " ".join(exec_params)

        # Execute shell
        call("ssh -t " + node.address + " '" +
             self.bin_dir + "/cassandra-cli" + params_str + "'",
             shell=True)

    def stop(self):
        self._check_initialization()

        logger.info("Stopping Cassandra")

        self.running_cassandra = False
        self.running = False

        pass

    def clean_logs(self):
        """Remove all Cassandra logs."""

        logger.info("Cleaning logs")

        restart = False
        if self.running:
            logger.warn("The cluster needs to be stopped before cleaning.")
            self.stop()
            restart = True

        action = Remote("rm -rf " + self.logs_dir + "/*", self.hosts)
        action.run()

        if restart:
            self.start()

    def clean(self):
        """Remove all files created by Cassandra."""

        if self.running:
            logger.warn("The cluster needs to be stopped before cleaning.")
            self.stop()

        self.clean_logs()

    def __force_clean(self):
        pass
