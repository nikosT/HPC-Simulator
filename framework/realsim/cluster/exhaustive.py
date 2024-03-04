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


class ClusterExhaustive(AbstractCluster):

    def __init__(self, nodes, cores_per_node):
        AbstractCluster.__init__(self, 
                                 nodes=nodes, 
                                 cores_per_node=cores_per_node)

    def next_state(self):
        """Execute the jobs in the execution list
        """

        # Find smallest remaining time
        min_rem_time = math.inf
        for unit in self.execution_list:
            for job in unit:
                if type(job) != EmptyJob and job.remaining_time < min_rem_time:
                    min_rem_time = job.remaining_time

        assert min_rem_time >= 0

        if min_rem_time == math.inf:
            print(f"Infinity : {self.waiting_queue} {self.execution_list}")
            raise RuntimeError("Execution list is empty but the waiting queue still has jobs.")

        # Increase the overall cluster runtime
        self.makespan += min_rem_time

        # Create a new execution list
        execution_list: List[List[Job]] = list()

        # Remove the smallest remaining time and if a job hits zero
        # then substitute it with an EmptyJob instance
        for execution_unit in self.execution_list:

            substitute_unit = list()
            empty_jobs = list()

            # Set as first job in substitute unit the non empty job
            # with the highest amount of binded cores
            max_binded_cores = -1

            for job in execution_unit:

                if type(job) != EmptyJob:
                    job.remaining_time -= min_rem_time
                    if job.remaining_time == 0:
                        # Record in logger
                        self.logger.job_finish(job)
                        # Convert to an empty job
                        job = EmptyJob(job)

                # We may have to downsize the unit
                # before adding it to the execution list we are building

                # Set the largest non empty job as first in substitute_unit
                if type(job) != EmptyJob:
                    if job.binded_cores > max_binded_cores:
                        substitute_unit.insert(0, job)
                        max_binded_cores = job.binded_cores
                    else:
                        substitute_unit.append(job)
                else:
                    empty_jobs.append(job)

            # If there is any job still executing
            if substitute_unit != []:
            
                if len(substitute_unit) > 1:

                    # Recalculate speedup of tail jobs
                    for job in substitute_unit[1:]:
                        if job.speedup != job.get_speedup(substitute_unit[0]):
                            job.ratioed_remaining_time(substitute_unit[0])

                    # Recalculate of head job
                    worst_job = min(substitute_unit[1:], key=(
                        lambda wjob: substitute_unit[0].get_speedup(wjob)
                    ))

                    if substitute_unit[0].speedup != substitute_unit[0].get_speedup(worst_job):
                        substitute_unit[0].ratioed_remaining_time(worst_job)

                # TODO: MAYBE DOWNSIZE THE EXECUTION UNIT
                # From empty jobs check if there is a job with amount of cores
                # higher than our first job in substitute_unit and swap their
                # number of binded cores
                for empty_job in empty_jobs:
                    if empty_job.binded_cores > substitute_unit[0].binded_cores:
                        swap = empty_job.binded_cores
                        empty_job.binded_cores = substitute_unit[0].binded_cores
                        substitute_unit[0].binded_cores = swap

            # Extend substitute_unit with empty_jobs
            substitute_unit.extend(empty_jobs)

            # Add the substitute_unit in the new execution list
            execution_list.append(substitute_unit)

        # Replace execution list
        self.execution_list = execution_list

    def free_resources(self):
        """Find any EmptyJob instances and return the resources
        to the cluster
        """

        execution_list = deepcopy_list(self.execution_list)

        for execution_unit in self.execution_list:

            finished = True

            for job in execution_unit:

                if type(job) != EmptyJob:
                    finished = False
                    break
                else:
                    self.finished_jobs.append(job.job_id)

            if finished:
                if len(execution_unit) == 1:
                    self.free_cores += execution_unit[0].binded_cores
                else:
                    # Sort the jobs in execution unit by their number of binded
                    # cores descendingly
                    sorted_exec_unit = sorted(execution_unit, 
                                              key=lambda job: job.binded_cores,
                                              reverse=True)
                    self.free_cores += 2 * sorted_exec_unit[0].binded_cores

                # Remove finished units
                execution_list.remove(execution_unit)

        # Execution list re-assignment
        self.execution_list = execution_list
