import os
import sys
from typing import Optional

sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../../../../"
)))

from realsim.jobs.jobs import Job, EmptyJob
from realsim.jobs.utils import deepcopy_list
from realsim.scheduler.coscheduler import Coscheduler, ScikitModel

from abc import ABC
import math


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

        procset = self.assign_nodes(job.full_node_cores, self.cluster.total_procs)

        # Check if the job can be allocated for compact execution
        if procset is not None:
            self.cluster.waiting_queue.remove(job)
            job.start_time = self.cluster.makespan
            job.binded_cores = job.full_node_cores
            job.assigned_cores = procset
            self.cluster.execution_list.append([job])
            self.cluster.total_procs -= procset
            # self.cluster.free_cores -= job.binded_cores

            return True

        else:
            return False

    def deploy(self) -> bool:

        deployed = False

        #print(f"BEFORE: Free cores = {self.cluster.free_cores}")
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
                deployed = True
                continue

            # Check if it is eligible for spread allocation
            res = self.allocation_as_spread(job)

            if res:
                self.after_deployment()
                deployed = True
                continue

            # Check if it is eligible for compact allocation
            res = self.allocation_as_compact(job)

            if res:
                self.after_deployment()
                deployed = True
                continue

            # Check if there is a waiting job that can pair up with the job
            # and that they are allowed to allocate in the cluster
            res = self.colocation_with_wjobs(job, waiting_queue)

            if res:
                self.after_deployment()
                deployed = True
                continue

            # All the allocation tries have failed. Return the job at the first
            # out position and reassign the waiting queue of the cluster
            waiting_queue.insert(0, job)
            self.cluster.waiting_queue = waiting_queue
            break

        #print(f"AFTER: Free cores = {self.cluster.free_cores}")
        return deployed

    def xunit_estimated_finish_time(self, xunit: list[Job]) -> float:
        """Estimated finish time for an xunit; meaning the maximum time it takes
        for all the jobs inside an xunit to finish

        + xunit: the execution unit being tested
        """

        estimations: list[float] = list()

        for job in xunit:
            if type(job) != EmptyJob:
                # Estimation is based on the worst speedup of a job
                estimate = job.wall_time / job.get_min_speedup() + job.start_time - self.cluster.makespan
                if estimate < 0:
                    print(estimate, job.start_time, job.wall_time / job.get_min_speedup(), self.cluster.makespan, job)
                estimations.append(estimate)

        return max(estimations)

    def backfill(self) -> bool:

        deployed = False

        if len(self.cluster.waiting_queue) <= 1:
            # If there are not alternatives bail out
            return False

        blocked_job = self.cluster.waiting_queue[0]

        execution_list = deepcopy_list(self.cluster.execution_list)
        # The blocked job can be co-scheduled
        # This means it can either fit inside an existing execution unit
        # or it waits until whole xunits finish execution

        # Xunits that the blocked job can fit in
        xunits_for_colocation = list()
        estimated_start_time_coloc = math.inf

        # Xunits that the job cannot fit in and they will be merged in order
        # for the job to have enough free space to execute properly
        xunits_for_merge = list()
        estimated_start_time_merge = math.inf

        for xunit in execution_list:
            # If it compact allocated then to the mergers
            if len(xunit) == 1:
                xunits_for_merge.append(xunit)
            else:
                head_job = xunit[0]
                last_job = xunit[-1] # possible idle job
                if blocked_job.half_node_cores <= max(len(head_job.assigned_cores), len(last_job.assigned_cores)):
                    xunits_for_colocation.append(xunit)
                else:
                    xunits_for_merge.append(xunit)

        # Starting with xunits to merge we sort them by estimated finish time
        xunits_for_merge.sort(key=lambda xunit: self.xunit_estimated_finish_time(xunit))
        aggr_cores = len(self.cluster.total_procs)

        for xunit in xunits_for_merge:
            if len(xunit) == 1:
                if len(xunit[0].assigned_cores) + aggr_cores >= 2 * blocked_job.half_node_cores:
                    estimated_start_time_merge = self.xunit_estimated_finish_time(xunit)
                    break
                else:
                    aggr_cores += len(xunit[0].assigned_cores)
            else:
                xunit_binded_cores = sum([
                    len(job.assigned_cores) for job in xunit
                ])

                if xunit_binded_cores + aggr_cores >= 2 * blocked_job.half_node_cores:
                    estimated_start_time_merge = self.xunit_estimated_finish_time(xunit)
                    break
                else:
                    aggr_cores += xunit_binded_cores

        # Estimate time with xunits for colocation
        estimations = list()
        for xunit in xunits_for_colocation:
            aggr_cores = 0
            xunit_copy = deepcopy_list(xunit)
            last_job = xunit_copy[-1]
            if type(last_job) == EmptyJob:
                xunit_copy.remove(last_job)
                aggr_cores = len(last_job.assigned_cores)

            xunit_copy.sort(key=lambda job: job.wall_time / job.get_min_speedup() + job.start_time - self.cluster.makespan)
            for job in xunit_copy:
                if len(job.assigned_cores) + aggr_cores >= blocked_job.half_node_cores:
                    estimations.append(job.wall_time / job.get_min_speedup() + job.start_time - self.cluster.makespan)
                    break
                else:
                    aggr_cores += len(job.assigned_cores)

        # The estimated start time is the minimum of the two options
        # to be coscheduled in an xunit or to create an xunit
        # if estimations != []:
        #     estimated_start_time_coloc = min(estimations)
        #     estimated_start_time = min(estimated_start_time_coloc, estimated_start_time_merge)
        # else:
        #     estimated_start_time = estimated_start_time_merge

        # In finding the possible backfillers
        waiting_queue = deepcopy_list(self.cluster.waiting_queue[1:])

        while waiting_queue != []:

            backfill_job = self.pop(waiting_queue)

            if estimated_start_time_coloc is not None and\
                    estimated_start_time_coloc < estimated_start_time_merge:

                res = self.colocation_to_xunit(backfill_job)

                if res:
                    self.after_deployment()
                    deployed = True
                    continue

                # Check if it is eligible for spread allocation
                res = self.allocation_as_spread(backfill_job)

                if res:
                    self.after_deployment()
                    deployed = True
                    continue

                # Check if it is eligible for compact allocation
                res = self.allocation_as_compact(backfill_job)

                if res:
                    self.after_deployment()
                    deployed = True
                    continue

                # Check if there is a waiting job that can pair up with the job
                # and that they are allowed to allocate in the cluster
                res = self.colocation_with_wjobs(backfill_job, waiting_queue)

                if res:
                    self.after_deployment()
                    deployed = True
                    continue

            else:
                if backfill_job.wall_time <= estimated_start_time_merge:

                    # Try to fit the job in an xunit
                    res = self.colocation_to_xunit(backfill_job)

                    if res:
                        self.after_deployment()
                        deployed = True
                        continue

                    # Check if it is eligible for spread allocation
                    res = self.allocation_as_spread(backfill_job)

                    if res:
                        self.after_deployment()
                        deployed = True
                        continue

                    # Check if it is eligible for compact allocation
                    res = self.allocation_as_compact(backfill_job)

                    if res:
                        self.after_deployment()
                        deployed = True
                        continue

                    # Check if there is a waiting job that can pair up with the job
                    # and that they are allowed to allocate in the cluster
                    res = self.colocation_with_wjobs(backfill_job, waiting_queue)

                    if res:
                        self.after_deployment()
                        deployed = True
                        continue

        return deployed
