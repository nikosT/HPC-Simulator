# Set path for local lib
import os
import sys

from procset import ProcSet
sys.path.append(
        os.path.abspath(os.path.join(os.path.dirname(__file__), '../../'))
)

from realsim.cluster.abstract import AbstractCluster
from realsim.jobs import Job, EmptyJob
from realsim.jobs.utils import deepcopy_list

import math


class ClusterExhaustive(AbstractCluster):

    def __init__(self, nodes, cores_per_node):
        AbstractCluster.__init__(self, 
                                 nodes=nodes, 
                                 cores_per_node=cores_per_node)

    def next_state(self):
        """Execute the jobs in the execution list
        """
        # print()
        # print(len(self.waiting_queue))
        # print()

        # Find smallest remaining time of executing jobs
        min_rem_time = math.inf
        for xunit in self.execution_list:
            for job in xunit:
                if type(job) != EmptyJob and job.remaining_time < min_rem_time:
                    min_rem_time = job.remaining_time

        # Find smallest remaining time for a job to show up in the waiting queue
        for job in self.database.preloaded_queue:
            showup_time = job.submit_time - self.makespan
            if showup_time > 0 and showup_time < min_rem_time:
                min_rem_time = showup_time

        assert min_rem_time >= 0

        if min_rem_time == math.inf and self.waiting_queue != []:
            print(f"Infinity : {self.free_cores}\n {self.waiting_queue} {self.execution_list}")
            raise RuntimeError("Execution list is empty but the waiting queue still has jobs.")

        # If there is an aging mechanism in the scheduling algorithm
        if self.scheduler.aging_enabled and len(self.waiting_queue) > 0 and self.waiting_queue[0].age < self.scheduler.age_threshold:
            # Find the interval until the next scheduler step
            scheduler_timer = int(self.makespan / self.scheduler.time_step) * self.scheduler.time_step
            next_shd_step = (scheduler_timer + self.scheduler.time_step) - self.makespan
            # Find how much time should pass for the head job to reach the
            # maximum age for compact allocation
            max_age_step = next_shd_step + (self.scheduler.age_threshold - (self.waiting_queue[0].age + 1)) * self.scheduler.time_step
            # If the time it takes to reach is less than the min_rem_time then
            # re-enact deployment
            if max_age_step < min_rem_time:
                min_rem_time = max_age_step
                self.waiting_queue[0].age = self.scheduler.age_threshold
                print(self.waiting_queue[0].job_id, self.waiting_queue[0].job_name, self.makespan)

        # Increase the overall cluster runtime
        self.makespan += min_rem_time

        # Increase the waiting/queued time of each job in the waiting queue
        for job in self.waiting_queue:
            job.waiting_time += min_rem_time

        # Create a new execution list
        execution_list: list[list[Job]] = list()

        # Remove the smallest remaining time and if a job hits zero
        # then substitute it with an EmptyJob instance
        for execution_unit in self.execution_list:

            substitute_unit = list()

            # Set as first job in substitute unit the non empty job
            # with the highest amount of binded cores
            max_binded_cores = -1
            idle_procs = ProcSet()

            # Calculate for all jobs
            for job in execution_unit:

                if type(job) != EmptyJob:
                    job.remaining_time -= min_rem_time
                    if job.remaining_time == 0:
                        # Record in logger
                        self.logger.evt_job_finishes(job)
                        # Convert to an empty job
                        job = EmptyJob(job)

                if type(job) != EmptyJob:
                    # Set the largest non empty job as first in substitute_unit
                    if len(job.assigned_cores) > max_binded_cores:
                        substitute_unit.insert(0, job)
                        max_binded_cores = len(job.assigned_cores)
                    else:
                    # Else put it as a tail job
                        substitute_unit.append(job)
                else:
                    # If it is an EmptyJob record the amount of idle cores
                    # idle_cores += len(job.assigned_cores)
                    idle_procs |= job.assigned_cores


            # If there is any job still executing
            if len(substitute_unit) > 1:

                sub_unit_cores = sum([len(job.assigned_cores) for job in substitute_unit])

                # If the number of idle cores is larger than the number of utilized
                # cores then all the remaining jobs in the xunit will be executing as spread
                if len(idle_procs) >= sub_unit_cores:
                    # surplass = idle_cores - sub_unit_cores
                    # self.free_cores += surplass
                    # idle_cores = sub_unit_cores
                    for job in substitute_unit:
                        if job.sim_speedup != job.get_max_speedup():
                            self.ratio_rem_time(job, 'max')
                            # job.remaining_time *= (job.sim_speedup / job.get_max_speedup())
                            # job.sim_speedup = job.get_max_speedup()
                else:
                # In contrast, the tail jobs share their resources with the head
                # job and vica versa

                    # Recalculate speedup of tail jobs
                    for job in substitute_unit[1:]:
                        if job.sim_speedup != self.database.heatmap[job.job_name][substitute_unit[0].job_name]:
                            # job.ratioed_remaining_time(substitute_unit[0])
                            self.ratio_rem_time(job, substitute_unit[0])

                    # Recalculate of head job
                    worst_job = min(substitute_unit[1:], key=(
                        lambda wjob: self.database.heatmap[substitute_unit[0].job_name][wjob.job_name]
                    ))

                    if substitute_unit[0].sim_speedup != self.database.heatmap[substitute_unit[0].job_name][worst_job.job_name]:
                        # substitute_unit[0].ratioed_remaining_time(worst_job)
                        self.ratio_rem_time(substitute_unit[0], worst_job)

            if len(idle_procs) > 0:
                # Create an empty job to occupy the idle cores of the xunit
                empty_job = EmptyJob(Job(None,
                                         -1,
                                         "idle",
                                         len(idle_procs),
                                         len(idle_procs),
                                         idle_procs,
                                         -1,
                                         -1,
                                         None, None, None, None
                                         ))
                # Extend substitute_unit with empty_jobs
                substitute_unit.append(empty_job)

            # Add the substitute_unit in the new execution list
            execution_list.append(substitute_unit)

        # Replace execution list
        self.execution_list = execution_list


    def free_resources(self):
        """Find any EmptyJob instances and return the resources
        to the cluster
        """

        execution_list = deepcopy_list(self.execution_list)

        for xunit in self.execution_list:

            if len(xunit) == 1 and type(xunit[0]) == EmptyJob:
                # self.free_cores += xunit[0].binded_cores
                self.total_procs |= xunit[0].assigned_cores
                execution_list.remove(xunit)

        # Execution list re-assignment
        self.execution_list = execution_list
