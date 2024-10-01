from abc import ABC, abstractmethod
import math
from functools import reduce

import os
import sys
from typing import Optional

from framework.realsim.compengine import ComputeEngine

sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), '../../')
))

sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), '../../../')
))

from realsim.cluster.host import Host
from realsim.scheduler.scheduler import Scheduler
from realsim.jobs import Job
from typing import Protocol


class ScikitModel(Protocol):
    def predict(self, X):
        pass


class Coscheduler(Scheduler, ABC):
    """Base class for all co-scheduling algorithms
    """

    name = "Abstract Co-Scheduler"
    description = "Abstract base class for all co-scheduling algorithms"

    def __init__(self,
                 backfill_enabled: bool = False,
                 aging_enabled: bool = False,
                 speedup_threshold: float = 1.0,
                 system_utilization: float = 1.0,
                 engine: Optional[ScikitModel] = None):

        Scheduler.__init__(self)

        self.backfill_enabled = backfill_enabled
        self.aging_enabled = aging_enabled
        self.speedup_threshold = speedup_threshold
        self.system_utilization = system_utilization

        self.engine = engine

    @abstractmethod
    def setup(self) -> None:
        pass

    @abstractmethod
    def waiting_job_candidates_reorder(self, job: Job, co_job: Job) -> float:
        pass

    @abstractmethod
    def xunit_candidates_reorder(self, job: Job, xunit: list[Job]) -> float:
        pass

    def after_deployment(self, *args):
        """After deploying a job in a pair or compact then some after processing
        events may need to take place. For example to calculate values necesary
        for the heuristics functions (e.g. the fragmentation of the cluster)
        """
        pass

    def coloc_condition(self, hostname: str, job: Job) -> float:
        """Condition on how to sort the 'in use' hosts/nodes
        The current implementation is sorting by the best worst speedup from the
        neighboring jobs
        """

        co_job_sigs = list(self.cluster.hosts[hostname].jobs.keys())

        # Get the worst possible speedup
        first_co_job_name = co_job_sigs[0].split(":")[-1]
        worst_speedup = self.database.heatmap[job.job_name][first_co_job_name]

        for co_job_sig in co_job_sigs[1:]:
            co_job_name = co_job_sig.split(":")[-1]
            speedup = self.database.heatmap[job.job_name][co_job_name]
            if speedup < worst_speedup:
                worst_speedup = speedup

        return worst_speedup

    def colocation(self, job: Job, socket_conf: tuple) -> bool:
        """We allocate first to the idle hosts and then to the in use hosts
        """

        job.socket_conf = socket_conf
        req_cores = job.num_of_processes
        needed_ppn = sum(job.socket_conf)

        # Get only the suitable hosts
        suitable_hosts = self.find_suitable_nodes(job.num_of_processes,
                                                  socket_conf)
        # If no suitable hosts where found
        if suitable_hosts == dict():
            return False

        # Start allocating to the idle hosts
        idle_hosts = [
                hostname 
                for hostname in suitable_hosts
                if self.cluster.hosts[hostname].state == Host.IDLE
        ]

        for hostname in idle_hosts:
            ComputeEngine.deploy_job_to_host(hostname, job, suitable_hosts[hostname])
            suitable_hosts.pop(hostname)

            req_cores -= needed_ppn
            if req_cores <= 0:
                break

        # If the job still needs hosts to allocate cores then continue to the
        # sorted by best, not idle hosts
        if req_cores > 0:

            # Sort the remaining hosts by best candidates
            rem_hostnames = sorted(list(suitable_hosts.keys()), 
                                   key=lambda hostname: self.coloc_condition(hostname, job),
                                   reverse=True)

            for hostname in rem_hostnames:
                ComputeEngine.deploy_job_to_host(hostname, job, suitable_hosts[hostname])
                if req_cores <= 0:
                    break

        return True

    @abstractmethod
    def deploy(self) -> bool:
        pass
