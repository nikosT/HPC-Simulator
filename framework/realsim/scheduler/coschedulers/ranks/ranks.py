import os
import sys
from typing import Optional

sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../../../../"
)))

from realsim.jobs.jobs import Job
from realsim.jobs.utils import deepcopy_list
from realsim.scheduler.coschedulerV2 import Coscheduler, ScikitModel

from abc import ABC


class RanksCoscheduler(Coscheduler, ABC):

    name = "Ranks Co-Scheduler"
    description = """Rank is the measure of how many good pairs a job can 
    construct. If a job reaches rank 0, then it is not capable for co-scheduling
    and is allocated for exclusive compact execution."""

    def __init__(self,
                 backfill_enabled: bool = False,
                 speedup_threshold: float = 1.0,
                 ranks_threshold: float = 1.0,
                 system_utilization: float = 1.0,
                 engine: Optional[ScikitModel] = None):

        Coscheduler.__init__(self, 
                             backfill_enabled, 
                             speedup_threshold, 
                             system_utilization, 
                             engine)

        self.ranks : dict[int, int] = dict() # jobId --> number of good pairings
        self.ranks_threshold = ranks_threshold

    def update_ranks(self):

        self.ranks = {job.job_id : 0 for job in self.cluster.waiting_queue}

        # Update ranks for each job
        for i, job in enumerate(self.cluster.waiting_queue):

            for co_job in self.cluster.waiting_queue[i+1:]:

                job_speedup = self.heatmap[job.job_name][co_job.job_name]
                co_job_speedup = self.heatmap[co_job.job_name][job.job_name]

                if job_speedup is None or co_job_speedup is None:
                    continue

                avg_speedup = (job_speedup + co_job_speedup) / 2

                if avg_speedup > self.ranks_threshold:
                    self.ranks[job.job_id] += 1
                    self.ranks[co_job.job_id] += 1

    def setup(self):

        # Create heatmap
        Coscheduler.setup(self)

        # Create ranks
        self.update_ranks()

    def after_deployment(self, *args):
        self.update_ranks()

    def allocation_as_compact(self, job: Job) -> bool:

        # The job is not eligible for compact execution
        if self.ranks[job.job_id] != 0:
            return False

        # Check if the job can be allocated for compact execution
        if job.full_node_cores <= self.cluster.free_cores:
            self.cluster.waiting_queue.remove(job)
            job.start_time = self.cluster.makespan
            job.binded_cores = job.full_node_cores
            self.cluster.execution_list.append([job])
            self.cluster.free_cores -= job.binded_cores

            return True

        else:
            return False

    def deploy(self) -> None:

        self.update_ranks()

        waiting_queue = deepcopy_list(self.cluster.waiting_queue)
        waiting_queue.sort(key=lambda job: self.waiting_queue_reorder(job),
                           reverse=True)

        while waiting_queue != []:

            # Remove from the waiting queue
            job = self.pop(waiting_queue)

            # Try to fit the job in an xunit
            res = self.colocation_to_xunit(job)

            if res:
                self.after_deployment()
                continue

            # Check if it is eligible for compact allocation
            res = self.allocation_as_compact(job)

            if res:
                self.after_deployment()
                continue

            # Check if there is a waiting job that can pair up with the job
            # and that they are allowed to allocate in the cluster
            res = self.colocation_with_wjobs(job, waiting_queue)

            if res:
                self.after_deployment()
                continue

            # All the allocation tries have failed. Return the job at the first
            # out position and reassign the waiting queue of the cluster
            waiting_queue.insert(0, job)
            self.cluster.waiting_queue = waiting_queue
            break

        return
