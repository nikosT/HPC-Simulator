import os
import sys
from typing import Optional

sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../../../../"
)))

from realsim.jobs import Job, EmptyJob
from .ranks import RanksCoscheduler, ScikitModel

from numpy import average as avg


class BalancingRanksCoscheduler(RanksCoscheduler):

    name = "Balancing Ranks Co-Scheduler"

    description = """Balancing is the method to achieve at least 1 overall
    speedup for the execution list that is deployed by the scheduling algorithm.
    The main metric ll_avg_speedup calculates the average speedup of the
    execution list as it is being constructed from the lefthand side. The
    specific implementation also has a metric in order to reduce the vertical 
    fragmentation of the execution list. The specific iteration of the balancing
    methodology uses the `ranks` deploying architecture.
    """

    def __init__(self, 
                 threshold: float = 1, 
                 engine: Optional[ScikitModel] = None, 
                 ranks_threshold: float = 1):
        self.ll_avg_speedup = 0
        self.ll_xunits_num = 0
        self.fragmentation = 0
        RanksCoscheduler.__init__(self, 
                                  threshold=threshold, 
                                  engine=engine,
                                  ranks_threshold=ranks_threshold)

    def xunits_order(self, xunit: list[Job]):
        return float(xunit[0].binded_cores)

    def xunits_candidates_order(self, largest_job: Job, job: Job):

        # Rank of job
        rank = self.ranks[job.job_id]
        if rank == 0:
            rank = -1
        # rank ratio
        rank_r = rank / len(self.cluster.waiting_queue)

        # Needed cores by the job
        needed_cores = self.cluster.half_node_cores(job)
        # cores ratio
        cores_r = needed_cores / largest_job.binded_cores
        # fragmentation ratio
        frag_r = cores_r * (1 - self.fragmentation) +\
                (1 - cores_r) * self.fragmentation

        # Average speedup between xunit's largest job and the job candidate
        avg_speedup = avg([
            self.heatmap[job.job_name][largest_job.job_name],
            self.heatmap[largest_job.job_name][job.job_name]
        ])
        # speedup ratio
        if self.ll_avg_speedup > 0:
            speedup_r = avg_speedup ** (2 / self.ll_avg_speedup)
        else:
            speedup_r = avg_speedup

        return rank_r * speedup_r * frag_r

    def waiting_queue_order(self, job: Job) -> float:

        rank_r = self.ranks[job.job_id] / len(self.cluster.waiting_queue)
        cores_r = self.cluster.free_cores / job.num_of_processes

        return rank_r * cores_r

    def wjob_candidates_order(self, job: Job, co_job: Job):

        # Rank of job
        rank = self.ranks[co_job.job_id]
        if rank == 0:
            rank = -1
        # rank ratio
        rank_r = rank / len(self.cluster.waiting_queue)

        # Needed cores by the job
        needed_cores = self.cluster.half_node_cores(co_job)
        # cores ratio
        cores_r = needed_cores / job.binded_cores
        # fragmentation ratio
        frag_r = cores_r * (1 - self.fragmentation) +\
                (1 - cores_r) * self.fragmentation

        # Average speedup between xunit's largest job and the job candidate
        avg_speedup = avg([
            self.heatmap[job.job_name][co_job.job_name],
            self.heatmap[co_job.job_name][job.job_name]
        ])
        # speedup ratio
        if self.ll_avg_speedup > 0:
            speedup_r = avg_speedup ** (2 / self.ll_avg_speedup)
        else:
            speedup_r = avg_speedup

        return rank_r * speedup_r * frag_r

    def xunit_avg_speedup(self, xunit: list[Job]) -> float:
        return float(avg([job.speedup for job in xunit]))

    def after_deployment(self, xunit: list[Job]):
        RanksCoscheduler.after_deployment(self, xunit)

        # Left-side list overall speedup metrics
        self.ll_avg_speedup = ((self.ll_avg_speedup * self.ll_xunits_num) +\
                self.xunit_avg_speedup(xunit)) / (self.ll_xunits_num + 1)
        self.ll_xunits_num += 1

        # Fragmentation metric
        binded_cores = self.cluster.total_cores - self.cluster.free_cores

        if binded_cores == 0:
            return

        prev_binded_cores = binded_cores - 2 * xunit[0].binded_cores

        added_empty_space = sum(map(
            lambda job: job.binded_cores,
            filter(lambda job: type(job) == EmptyJob, xunit)
        ))

        self.fragmentation = ((self.fragmentation * prev_binded_cores) +\
                added_empty_space) / binded_cores
