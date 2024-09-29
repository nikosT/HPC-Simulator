from __future__ import annotations
from abc import ABC, abstractmethod
from typing import TypeVar, Generic, TYPE_CHECKING, Optional
import os
import sys
import math
from functools import reduce

from procset import ProcSet

sys.path.append(os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "../../../"
        )
    ))

from realsim.jobs import Job
from realsim.database import Database
from realsim.logger.logger import Logger
from realsim.compengine import ComputeEngine

if TYPE_CHECKING:
    from realsim.cluster.abstract import AbstractCluster

Cluster = TypeVar("Cluster", bound="AbstractCluster")


class Scheduler(ABC, Generic[Cluster]):
    """Scheduler provides a base class for scheduling methods. When creating an
    instance of Cluster a scheduler must always be provided and a reference to
    the cluster should also be provided to the scheduler instance. This way both
    instances can communicate their data immediately.
    """

    # The name of the scheduling algorithm
    name = "Abstract Scheduler"

    # Describe the philosophy of the scheduler
    description = "The abstract base class for all scheduling algorithms"

    def __init__(self, backfill_enabled: bool = False):

        self.database: Database
        self.cluster: Cluster
        self.logger: Logger

        # Variable to test whether a backfill policy is enabled
        self.backfill_enabled: bool = backfill_enabled
        self.backfill_depth: int = 100
        self.aging_enabled: bool = False
        self.age_threshold: int = 10
        self.time_step = 30 # Decide every 30 seconds 
        self.timer = self.time_step

    def assign_database(self, db: Database) -> None:
        """The database stores useful information for a scheduling algorithm
        that can be used as advice for better decision making.
        """
        self.database = db

    def assign_cluster(self, cluster: Cluster) -> None:
        """This method is called from a cluster instance
        when it is created. It can also be used to reassign
        the scheduler to other clusters. It is essential for
        rapid experimenting.
        """
        self.cluster = cluster

    def assign_logger(self, logger: Logger) -> None:
        self.logger = logger

    def find_suitable_nodes(self, 
                            req_cores: int, 
                            socket_conf: tuple) -> dict[str, list[ProcSet]]:
        """ Returns hosts and their procsets that a job can use as resources
        + req_cores   : required cores for the job
        + socket_conf : under a certain socket mapping/configuration
        """
        cores_per_host = sum(socket_conf)
        to_be_allocated = dict()
        for hostname, host in self.cluster.hosts.items():
            # If under the specifications of the required cores and socket 
            # allocation
            if reduce(lambda x, y: x[0] <= len(x[1]) and y[0] <= len(y[1]), list(zip(socket_conf, host.sockets))):
                req_cores -= cores_per_host
                to_be_allocated.update({hostname: [
                    ProcSet.from_str(' '.join([str(x) for x in p_set[:socket_conf[i]]]))
                    for i, p_set in enumerate(host.sockets)]
                })

        # If the amount of cores needed is covered then return the list of possible
        # hosts
        if req_cores <= 0:
            return to_be_allocated
        # Else, if not all the cores can be allocated return an empty list
        else:
            return {}

    def compact_allocation(self, job: Job) -> bool:

        # Mark job as compact
        job.socket_conf = self.cluster.full_socket_allocation

        # Get number of required cores
        req_cores = job.num_of_processes

        # Find suitable hosts
        suitable_hosts = self.find_suitable_nodes(req_cores,
                                                  self.cluster.full_socket_allocation)

        #
        # Add filter to the suitable hosts (maybe general method for coloc)
        #

        # Can't allocate job
        if suitable_hosts == {}:
            return False

        needed_ppn = sum(self.cluster.full_socket_allocation)
        for hostname, psets in suitable_hosts.items():

            # Deploy job to hosts
            ComputeEngine.deploy_job_to_host(hostname, job, psets)

            req_cores -= needed_ppn
            if req_cores <= 0:
                break

        # Calculate the remaining time of the job
        ComputeEngine.calculate_job_rem_time(job)

        return True



    def pop(self, queue: list[Job]) -> Job:
        """Get and remove an object from a queue
        """
        obj = queue[0]
        queue.remove(obj)
        return obj

    @abstractmethod
    def setup(self) -> None:
        """Basic setup of scheduling algorithm before the start of the
        simulation
        """
        pass

    def backfill(self) -> bool:
        """A backfill algorithm for the scheduler
        """
        return False
        
    def waiting_queue_reorder(self, job: Job) -> float:
        """Waiting queue reordering logic
        """
        return 1.0

    @abstractmethod
    def deploy(self) -> bool:
        """Abstract method to deploy the new execution list to the cluster
        """
        pass
