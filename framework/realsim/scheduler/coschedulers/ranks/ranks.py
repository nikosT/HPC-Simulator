import os
import sys

sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../../../../"
)))

from realsim.jobs.jobs import Job
from realsim.jobs.utils import deepcopy_list
from realsim.scheduler.coscheduler import Coscheduler

from numpy import average as avg
from abc import ABC


class RanksCoscheduler(Coscheduler, ABC):

    name = "Abstract Ranks Co-Scheduler"
    description = """Deploying architecture with ranks as a fallback mechanism
    to default scheduling"""

    def __init__(self, 
                 threshold: int = 1, 
                 engine=None,
                 ranks_threshold: int = 1):

        # If the ranks_threshold is lower than threshold then the co-scheduler
        # would hint that there are more neighbors than the actual number of
        # candidates provided leading to waiting jobs that might stall for
        # co-scheduling but never being deployed for execution
        if ranks_threshold < threshold:
            raise RuntimeError("""Ranks threshold should always be greater than
                               co-scheduling threshold in order to not fall to
                               an infinite loop in the simulation loop""")

        self.ranks : dict[int, int] = dict() # jobId --> good pairings
        self.ranks_threshold = ranks_threshold
        Coscheduler.__init__(self, threshold=threshold, engine=engine)

    def update_ranks(self):

        self.ranks = {job.job_id : 0 for job in self.cluster.waiting_queue}

        # Update ranks for each job
        for i, job in enumerate(self.cluster.waiting_queue):

            for co_job in self.cluster.waiting_queue[i+1:]:

                job_speedup = self.heatmap[job.job_name][co_job.job_name]
                co_job_speedup = self.heatmap[co_job.job_name][job.job_name]

                if job_speedup is None or co_job_speedup is None:
                    continue

                avg_speedup = avg([job_speedup, co_job_speedup])

                if avg_speedup > self.ranks_threshold:
                    self.ranks[job.job_id] += 1
                    self.ranks[co_job.job_id] += 1

    def setup(self):

        # Create heatmap
        Coscheduler.setup(self)

        # Create ranks
        self.update_ranks()

    def after_deployment(self, xunit: list[Job]):
        self.update_ranks()

    def deploying_wait_compact(self, deploying_list):

        waiting_queue: list[Job] = deepcopy_list(self.cluster.waiting_queue)

        # Deploy any job that has 0 ranking in compact allocation policy
        for job in waiting_queue:
            if self.ranks[job.job_id] == 0 and self.cluster.full_node_cores(job) <= self.cluster.free_cores:

                        job.binded_cores = self.cluster.full_node_cores(job)

                        # Deployment!
                        deploying_list.append([job])

                        self.cluster.free_cores -= job.binded_cores
                        # Remove from the waiting queue
                        self.cluster.waiting_queue.remove(job)

                        self.deploying = True
                        self.after_deployment([job])


        return

    def deploy(self):

        # Reset deploying flag
        self.deploying = False

        # List of jobs to deploy
        deploying_list: list[list[Job]] = list()

        # First of all deploy the filled xunits
        deploying_list.extend(self.cluster.filled_xunits())

        # Call after-processing method for deployment for each xunit
        map(lambda xunit: self.after_deployment(xunit), deploying_list)

        # Co-scheduling waiting jobs with nonfilled executing units
        self.deploying_to_xunits(deploying_list)

        # Default (compact) scheduling of waiting jobs with rank equal to zero
        self.deploying_wait_compact(deploying_list)
        
        # Co-scheduling between waiting jobs
        self.deploying_wait_pairs(deploying_list)

        # Re-assign the execution list of the cluster
        self.cluster.execution_list = deploying_list

        # If there are new deployed jobs from the waiting queue then return True
        if self.deploying:
            # Logger cluster events update
            self.logger.cluster_events["deploying:success"] += 1
            return True

        # If no new waiting job is to be deployed then update Logger and return
        # False
        self.logger.cluster_events["deploying:failed"] += 1
        return False
