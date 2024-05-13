from __future__ import annotations
from abc import ABC, abstractmethod
from typing import TypeVar, Generic, TYPE_CHECKING
import os
import sys

from procset import ProcSet

sys.path.append(os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "../../../"
        )
    ))

from realsim.jobs import Job
from realsim.logger.logger import Logger

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

    def __init__(self):
        # Variable to test whether a backfill policy is enabled
        self.backfill_enabled = False

    def assign_cluster(self, cluster: Cluster) -> None:
        """This method is called from a cluster instance
        when it is created. It can also be used to reassign
        the scheduler to other clusters. It is essential for
        rapid experimenting.
        """
        self.cluster = cluster

    def assign_logger(self, logger: Logger) -> None:
        self.logger = logger

    def assign_procs(self, asked_cores: int) -> ProcSet:
        """Return a ProcSet from the total procs left in the cluster
        """
        # Remove intervals that have length lower than the number of cores per
        # node because it cannot constitue a node
        assignable_procs = list(filter(
            lambda procint:
            len(procint) >= self.cluster.cores_per_node,
            self.cluster.total_procs.intervals()
        ))

        assignable_procs = ProcSet.from_str(" ".join([
            str(procint) for procint in assignable_procs
        ]))

        binded_procs = assignable_procs[:asked_cores]
        constr_procset = ProcSet.from_str(" ".join([
            str(processor) for processor in binded_procs
        ]))

        return constr_procset

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

    def backfill(self) -> None:
        """A backfill algorithm for the scheduler
        """
        return
        
    def waiting_queue_reorder(self, job: Job) -> float:
        """Waiting queue reordering logic
        """
        return 1.0

    @abstractmethod
    def deploy(self) -> None:
        """Abstract method to deploy the new execution list to the cluster
        """
        pass
