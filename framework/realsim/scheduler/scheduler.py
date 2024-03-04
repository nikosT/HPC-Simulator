from __future__ import annotations
import abc
from typing import List, TypeVar, Generic, TYPE_CHECKING
from numpy import average as avg, floating
import os
import sys

sys.path.append(os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "../../../"
        )
    ))

from realsim.jobs import Job, EmptyJob
from realsim.jobs.utils import deepcopy_list
from realsim.logger.logger import Logger

if TYPE_CHECKING:
    from realsim.cluster.abstract import AbstractCluster
import realsim.jobs.utils as jutils

Cluster = TypeVar("Cluster", bound="AbstractCluster")


class Scheduler(abc.ABC, Generic[Cluster]):
    """Scheduler provides a base class for scheduling methods. When creating an
    instance of Cluster a scheduler must always be provided and a reference to
    the cluster should also be provided to the scheduler instance. This way both
    instances can communicate their data immediately.
    """

    # The name of the scheduling algorithm
    name = "Abstract Scheduler"

    # Describe the philosophy of the scheduler
    description = "The abstract base class for all scheduling algorithms"

    def assign_cluster(self, cluster: Cluster):
        """This method is called from a cluster instance
        when it is created. It can also be used to reassign
        the scheduler to other clusters. It is essential for
        rapid experimenting.
        """
        self.cluster = cluster

    def assign_logger(self, logger: Logger):
        self.logger = logger

    def pop(self, queue: List[Job]):
        """Get and remove an object from a queue
        """
        obj = queue[0]
        queue.remove(obj)
        return obj

    def backfill(self) -> None:
        """Rearrange jobs in waiting queue. Don't know 
        if a backfill algorithm will be used
        """
        pass

    @abc.abstractmethod
    def deploying(self) -> bool:
        """
        Deploying jobs from the waiting queue to the execution list
        """
        return False


