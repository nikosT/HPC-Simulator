import os
import sys
from typing import Optional

sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../../../../"
)))

from realsim.jobs import Job, EmptyJob
from .ranks import RanksCoscheduler, ScikitModel

from numpy import average as avg
import math


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
                 backfill_enabled: bool = False,
                 aging_enabled: bool = False,
                 # speedup_threshold: float = 1.0,
                 # ranks_threshold: float = 1.0,
                 system_utilization: float = 1.0,
                 engine: Optional[ScikitModel] = None):

        RanksCoscheduler.__init__(self,
                                  backfill_enabled=backfill_enabled,
                                  aging_enabled=aging_enabled,
                                  # speedup_threshold=speedup_threshold,
                                  # ranks_threshold=ranks_threshold,
                                  speedup_threshold=0.1,
                                  ranks_threshold=0.1,
                                  system_utilization=system_utilization,
                                  engine=engine)

        # Average execution units speedup of current execution list
        self.avg_xunits_speedup = 1

        self.system_load = 0

        # Inner fragmentation is the fragmentation of cores inside nodes (xunits
        # specifically)
        # Can be used to promote jobs with high rank that can glue to fragmented
        # xunits
        self.inner_frag = 0

    def waiting_queue_reorder(self, job: Job) -> float:

        # Jobs that have waited long to higher priority
        waiting_time = job.waiting_time if job.waiting_time != 0 else 1.0

        # What about estimated finish time interval (wall time)? We need jobs
        # that finish quick when the system load is high
        wall_time = job.wall_time

        # Response to how long it will take for a job to execute
        # We generally want low values for this metric
        response = waiting_time / wall_time

        # Are cores required important? If the system load is low we do not care
        # If it is high then we need jobs with low core requirements
        # cores_r = job.num_of_processes / self.cluster.total_cores
        cores_r = len(self.cluster.total_procs) / job.num_of_processes
        cores_r = (1 - cores_r) * self.system_load + cores_r * (1 - self.system_load)

        # Is job's overall speedup important? Maybe to increase the average
        # speedup of the execution list
        speedup = job.get_avg_speedup()
        # How important is speedup in the current state? If the value of average
        # speedup of all xunits is low then promote jobs that can increase the
        # value. Else do the opossite
        speedup_r = speedup ** (2 / self.avg_xunits_speedup)

        # What about ranks? We need high ranking jobs to spend more time in the
        # execution list for more potential pairs
        rank = self.ranks[job.job_id]
        rank_r = rank / len(self.cluster.waiting_queue)

        #return response * cores_r * speedup_r * rank_r
        return waiting_time

    def waiting_job_candidates_reorder(self, job: Job, co_job: Job) -> float:

        # Co-jobs that have waited long to higher priority
        waiting_time = co_job.waiting_time if co_job.waiting_time != 0 else 1.0

        # If the inner fragmentation high then we need co-jobs close to the same requirements as job
        # As inner frag reaches 1 then cores_r must promote co_jobs that return 1 as value
        cores_r = co_job.half_node_cores / job.half_node_cores
        #cores_r = self.inner_frag / cores_r

        if self.database.heatmap[job.job_name][co_job.job_name] is not None:
            speedup = (self.database.heatmap[job.job_name][co_job.job_name] + self.database.heatmap[co_job.job_name][job.job_name]) / 2.0
            speedup_r = speedup# ** (2 / self.avg_xunits_speedup)
        else:
            speedup_r = 1.0

        # We want co-jobs with high rank because they will be good neighbors
        # to the next jobs in the waiting queue
        rank = self.ranks[co_job.job_id]
        rank_r = rank / len(self.cluster.waiting_queue)

        #return waiting_time * speedup_r
        return waiting_time * (speedup_r ** 0.5)

    def xunit_candidates_reorder(self, job: Job, xunit: list[Job]) -> float:

        head_job = xunit[0]
        idle_job = xunit[-1]

        # Maximum will always be the number of idle binded cores
        cores_r = job.half_node_cores / len(idle_job.assigned_cores)
        cores_r = cores_r if cores_r != 0 else 1

        if job.half_node_cores > len(head_job.assigned_cores):
            # worst_neighbor = min(xunit, key=lambda neighbor: job.get_speedup(neighbor) if type(neighbor) != EmptyJob else math.inf)
            worst_neighbor = min(xunit, 
                                 key=lambda neighbor: 
                                 self.database.heatmap[job.job_name][neighbor.job_name] 
                                 if type(neighbor) != EmptyJob and self.database.heatmap[job.job_name][neighbor.job_name] is not None 
                                 else math.inf)
            speedup = (self.database.heatmap[job.job_name][worst_neighbor.job_name] + self.database.heatmap[worst_neighbor.job_name][job.job_name]) / 2.0
        else:
            speedup = (self.database.heatmap[job.job_name][head_job.job_name] + self.database.heatmap[head_job.job_name][job.job_name]) / 2.0
        
        speedup_r = speedup ** (2 / self.avg_xunits_speedup)

        return speedup_r * cores_r
        # return speedup_r / cores_r

    def after_deployment(self, *args):

        RanksCoscheduler.after_deployment(self)

        # Calculate average xunit speedup and inner fragmentation
        xunit_speedup: list[float] = list()
        inner_frags: list[float] = list()
        for xunit in self.cluster.execution_list:

            if len(xunit) == 1:
                xunit_speedup.append(1.0)
                inner_frags.append(0.0)
                continue

            xunit_jobs_speedups: list[float] = list()

            for job in xunit:
                if type(job) != EmptyJob:
                    # In reality we only know the heatmap, overall and max
                    # speedups so knowledge of runtime speedup is unknown
                    xunit_jobs_speedups.append(job.get_avg_speedup())

            # Calculate co-scheduled xunit's average overall speedup
            xunit_speedup.append(float(avg(xunit_jobs_speedups)))

            # Calculate co-scheduled xunit's inner fragmentation
            idle_job: Job = xunit[-1]
            total_binded_cores = sum([len(job.assigned_cores) for job in xunit])
            inner_frags.append(len(idle_job.assigned_cores) / total_binded_cores)

        self.avg_xunits_speedup = float(avg(xunit_speedup))
        self.system_load = 1.0 - len(self.cluster.total_procs) / self.cluster.total_cores
        self.inner_frag = float(avg(inner_frags))

