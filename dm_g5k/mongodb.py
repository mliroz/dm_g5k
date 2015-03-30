import os
import shutil
from subprocess import call
import tempfile

from ConfigParser import ConfigParser

from execo.action import TaktukPut, Get, Remote, TaktukRemote, \
    SequentialActions
from execo_engine import logger
from execo_g5k.api_utils import get_host_cluster

from dm_g5k.cluster import Cluster

# Configuration files
CONF_FILE = "mongodb.conf"

# Default parameters
DEFAULT_MONGODB_BASE_DIR = "/tmp/mongodb"
DEFAULT_MONGODB_DATA_DIR = DEFAULT_MONGODB_BASE_DIR + "/data"
DEFAULT_MONGODB_CONF_DIR = DEFAULT_MONGODB_BASE_DIR + "/conf"
DEFAULT_MONGODB_LOGS_FILE = DEFAULT_MONGODB_BASE_DIR + "/mongodb.log"

DEFAULT_MONGODB_PORT = 5586

DEFAULT_MONGODB_LOCAL_CONF_DIR = "conf"


class MongoDBException(Exception):
    pass


class MongoDBNotInitializedException(MongoDBException):
    pass


class MongoDBCluster(Cluster):
    """This class manages the whole life-cycle of a MongoDB cluster.
    """

    # Default properties
    defaults = {
        "mongodb_base_dir": DEFAULT_MONGODB_BASE_DIR,
        "mongodb_data_dir": DEFAULT_MONGODB_DATA_DIR,
        "mongodb_conf_dir": DEFAULT_MONGODB_CONF_DIR,
        "mongodb_logs_file": DEFAULT_MONGODB_LOGS_FILE,
        "mongodb_port": str(DEFAULT_MONGODB_PORT),

        "local_base_conf_dir": DEFAULT_MONGODB_LOCAL_CONF_DIR
    }

    def __init__(self, hosts, config_file=None):
        """Create a new MongoDB cluster with the given hosts.

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

        self.base_dir = config.get("cluster", "mongodb_base_dir")
        self.data_dir = config.get("cluster", "mongodb_data_dir")
        self.conf_dir = config.get("cluster", "mongodb_conf_dir")
        self.logs_file = config.get("cluster", "mongodb_logs_file")
        self.port = config.getint("cluster", "mongodb_port")
        self.local_base_conf_dir = config.get("local", "local_base_conf_dir")

        self.bin_dir = self.base_dir + "/bin"

        # Configure nodes
        self.hosts = hosts
        self.master = hosts[0]

        # Store cluster information
        self.host_clusters = {}
        for h in self.hosts:
            g5k_cluster = get_host_cluster(h)
            if g5k_cluster in self.host_clusters:
                self.host_clusters[g5k_cluster].append(h)
            else:
                self.host_clusters[g5k_cluster] = [h]

        logger.info("MongoDB cluster created with hosts " + str(self.hosts))

    def bootstrap(self, tar_file):
        """Install MongoDB in all cluster nodes from the specified tgz file.

        Args:
          mongodb_tar_file (str):
            The file containing MongoDB binaries.
        """

        # 1. Copy hadoop tar file and uncompress
        logger.info("Copy " + tar_file + " to hosts and uncompress")
        rm_files = TaktukRemote("rm -rf " + self.base_dir +
                                " " + self.conf_dir +
                                " " + self.data_dir +
                                " " + self.logs_file,
                                self.hosts)

        put_tar = TaktukPut(self.hosts, [tar_file], "/tmp")
        tar_xf = TaktukRemote("tar xf /tmp/" + os.path.basename(tar_file) +
                              " -C /tmp", self.hosts)
        SequentialActions([rm_files, put_tar, tar_xf]).run()

        # 2. Move installation to base dir
        logger.info("Create installation directories")
        action = Remote(
            "mv /tmp/" +
            os.path.basename(tar_file).replace(".tgz", "") + " " +
            self.base_dir,
            self.hosts)
        action.run()

        # 3 Create other dirs
        mkdirs = TaktukRemote("mkdir -p " + self.data_dir +
                              " && mkdir -p " + self.conf_dir +
                              " && touch " + os.path.join(self.conf_dir,
                                                          CONF_FILE),
                              self.hosts)
        mkdirs.run()

    def initialize(self):
        """Initialize the cluster: copy base configuration."""

        self._pre_initialize()

        logger.info("Initializing MongoDB")

        # Set basic configuration
        self._copy_base_conf()
        self._create_master_and_slave_conf()

        # Configure hosts depending on resource type
        for g5k_cluster in self.host_clusters:
            hosts = self.host_clusters[g5k_cluster]
            self._configure_servers(hosts)
            self._copy_conf(self.temp_conf_dir, hosts)

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

        self.temp_conf_dir = tempfile.mkdtemp("", "mongodb-", "/tmp")
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

        action = Get([self.master], remote_missing_files, self.temp_conf_dir)
        action.run()

    def _create_master_and_slave_conf(self):
        """Create master and slaves configuration files."""
        pass

    def _check_initialization(self):
        """ Check whether the cluster is initialized and raise and exception if
        not.
        """

        if not self.initialized:
            logger.error("The cluster should be initialized")
            raise MongoDBNotInitializedException(
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

        logger.info("Starting MongoDB")

        if self.running:
            logger.warn("MongoDB was already started")
            return

        proc = TaktukRemote(self.bin_dir + "/mongod "
                            "--fork "
                            "--dbpath " + self.data_dir + " "
                            "--port " + str(self.port) + " "
                            "--config " + os.path.join(self.conf_dir,
                                                       CONF_FILE) + " "
                            "--logpath " + self.logs_file,
                            self.hosts)
        proc.run()

        if not proc.finished_ok:
            logger.warn("Error while starting MongoDB")
        else:
            self.running = True

    def start_shell(self, node=None):
        """Open a MongoDB shell.

        Args:
          node (Host, optional):
            The host were the shell is to be started. If not provided,
            self.master is chosen.
        """

        self._check_initialization()

        if not node:
            node = self.master

        call("ssh -t " + node.address + " " +
             self.bin_dir + "/mongo --port " + str(self.port),
             shell=True)

    def stop(self):
        self._check_initialization()

        logger.info("Stopping MongoDB")

        proc = TaktukRemote(self.bin_dir + "/mongod "
                            "--shutdown "
                            "--dbpath " + self.data_dir,
                            self.hosts)
        proc.run()

        self.running = False

    def clean_logs(self):
        """Remove all MongoDB logs."""

        logger.info("Cleaning logs")

        restart = False
        if self.running:
            logger.warn("The cluster needs to be stopped before cleaning.")
            self.stop()
            restart = True

        action = Remote("rm -f " + self.logs_file, self.hosts)
        action.run()

        if restart:
            self.start()

    def clean_data(self):
        """Remove all data created by Hadoop (including filesystem)."""

        if self.running:
            logger.warn("The cluster needs to be stopped before cleaning.")
            self.stop()

        logger.info("Cleaning MongoDB data")

        restart = False
        if self.running:
            self.stop()
            restart = True

        action = Remote("rm -rf " + self.data_dir, self.hosts)
        action.run()

        if restart:
            self.start()

    def clean(self):
        """Remove all files created by MongoDB."""

        if self.running:
            logger.warn("The cluster needs to be stopped before cleaning.")
            self.stop()

        self.clean_logs()
        self.clean_data()

    def __force_clean(self):
        pass


