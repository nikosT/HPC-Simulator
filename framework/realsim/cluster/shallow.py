# Set path for local lib
import os
import sys
sys.path.append(
        os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
)

from realsim.cluster.abstract import AbstractCluster
from realsim.jobs import Job, EmptyJob
from realsim.jobs.utils import deepcopy_list

from typing import List
import math

class ClusterShallow(AbstractCluster):

    def __init__(self, nodes, cores_per_node):
        AbstractCluster.__init__(self, 
                                 nodes=nodes, 
                                 cores_per_node=cores_per_node)

    def next_state(self):
        """Execute the jobs in the execution list
        """

        # Find smallest remaining time
        min_rem_time = math.inf
        for jobs in self.execution_list:
            for job in jobs:
                if type(job) != EmptyJob and job.remaining_time < min_rem_time:
                    min_rem_time = job.remaining_time

        if min_rem_time == math.inf:
            print(f"Infinity : {self.waiting_queue}")
            return

        # Increase the overall cluster runtime
        self.makespan += min_rem_time

        # Create a new execution list
        execution_list: List[List[Job]] = list()

        # Remove the smallest remaining time and if a job hits zero
        # then substitute it with an EmptyJob instance
        for item in self.execution_list:

            # If it is a pair of colocated jobs
            if len(item) == 2:

                job0 = item[0]
                job1 = item[1]

                if type(job0) != EmptyJob:
                    job0.remaining_time -= min_rem_time
                    if job0.remaining_time == 0:
                        self.logger.job_finish(job0)
                        job0 = EmptyJob(job0)

                if type(job1) != EmptyJob:
                    job1.remaining_time -= min_rem_time
                    if job1.remaining_time == 0:
                        self.logger.job_finish(job1)
                        job1 = EmptyJob(job1)

                # The non empty job should always be first in the pair
                # If the partner is an EmptyJob then rebind the number
                # of cores for the first job and recalculate the number
                # of free cores in cluster
                if type(job0) == EmptyJob and type(job1) != EmptyJob:
                    self.free_cores += 2 * job1.binded_cores
                    job1.binded_cores = self.half_node_cores(job1)
                    job0.binded_cores = job1.binded_cores
                    self.free_cores -= 2 * job1.binded_cores
                    execution_list.append([job1, job0])
                else:
                    if type(job0) != EmptyJob and type(job1) == EmptyJob:
                        self.free_cores += 2 * job0.binded_cores
                        job0.binded_cores = self.half_node_cores(job0)
                        job1.binded_cores = job0.binded_cores
                        self.free_cores -= 2 * job0.binded_cores
                    execution_list.append([job0, job1])

            # If it is a standalone job
            elif len(item) == 1:

                job = item[0]

                if type(job) != EmptyJob:
                    job.remaining_time -= min_rem_time
                    if job.remaining_time == 0:
                        self.logger.job_finish(job)
                        job = EmptyJob(job)

                execution_list.append([job])

            else:
                raise RuntimeError("Found job in execution list that is neither alone or in a pair")

        # Replace execution list
        self.execution_list = execution_list

    def free_resources(self):
        """Find any EmptyJob instances and return the resources
        to the cluster
        """

        execution_list = deepcopy_list(self.execution_list)

        for jobs in self.execution_list:
            # If it is a pair
            if len(jobs) == 2:
                if type(jobs[0]) == EmptyJob and type(jobs[1]) == EmptyJob:
                    self.free_cores += 2 * jobs[0].binded_cores
                    execution_list.remove(jobs)
                    self.finished_jobs.extend([jobs[0].job_id, jobs[1].job_id])
                    self.logger.job_finish(jobs[0])
                    self.logger.job_finish(jobs[1])

            # If it is a compact job
            if len(jobs) == 1:
                if type(jobs[0]) == EmptyJob:
                    self.free_cores += jobs[0].binded_cores
                    execution_list.remove(jobs)
                    self.finished_jobs.append(jobs[0].job_id)
                    self.logger.job_finish(jobs[0])

        self.execution_list = execution_list
