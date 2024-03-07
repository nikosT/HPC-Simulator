import os
import sys
sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), '../../')
))


from numpy.random import seed, randint
from time import time_ns

from realsim.jobs import Job, EmptyJob
from realsim.jobs.utils import deepcopy_list
from realsim.scheduler.balancerFullOn import BalancerFullOn


class RandomScheduler(BalancerFullOn):

    name = "Random Co-Scheduler"
    description = "Random pairs co-scheduler"

    def wq_sort(self, job: Job):
        seed(time_ns() % (2 ** 32))
        return randint(len(self.cluster.waiting_queue))

    def coloc_ordering(self, job: Job, cojob: Job, ll_avg_speedup) -> float:
        seed(time_ns() % (2 ** 32))
        return float(randint(len(self.cluster.waiting_queue)))

    def deploying_exec_pairs(self, deploy_list,  ll_num, ll_avg_speedup):

        ########################################
        # Colocation with executing half pairs #
        ########################################

        nonfilled_units = self.nonfilled_exec_units()

        nonfilled_units.sort(
                key=lambda unit: unit[0].binded_cores,
                reverse=True
        )

        for unit in nonfilled_units:

            # Get the largest job's binded cores
            max_binded_cores = unit[0].binded_cores

            # Get the sum of binded cores of the smallest jobs
            min_binded_cores = 0
            for job in unit[1:]:
                if type(job) != EmptyJob:
                    min_binded_cores += job.binded_cores

            # Find jobs in waiting queue that can fit inside the unit
            fit_jobs = list(
                    filter(
                        lambda job:
                        job.load.full_load_name in unit[0].load.coloads and
                        self.cluster.half_node_cores(job)
                        <= (max_binded_cores - min_binded_cores),
                        self.cluster.waiting_queue
                    )
            )

            # If no candidate was found then check if there is only one non
            # empty job inside the unit
            if fit_jobs == []:

                if min_binded_cores == 0: 
                    # Change to spread execution
                    if unit[0].speedup != unit[0].get_max_speedup():
                        unit[0].remaining_time *= unit[0].speedup / unit[0].get_max_speedup()
                        unit[0].speedup = unit[0].get_max_speedup()

                    ll_avg_speedup = (ll_avg_speedup * ll_num +
                                      unit[0].speedup) / (ll_num + 1)
                    ll_num += 1
                else:
                    # If there are other jobs except the largest that are still
                    # executing then find the job that minimizes the largest's
                    # job speedup
                    lower_jobs = list(filter(
                        lambda job: type(job) != EmptyJob,
                        unit[1:]
                    ))

                    worst_job = min(lower_jobs, key=(lambda job:
                                                     unit[0].get_speedup(job)))

                    if unit[0].speedup != unit[0].get_speedup(worst_job):
                        unit[0].ratioed_remaining_time(worst_job)

                continue

            # Sort candidate jobs by an ordering function
            fit_jobs.sort(
                    key=(lambda cojob: self.coloc_ordering(unit[0], cojob, 
                                                           ll_avg_speedup)),
                    reverse=True
            )

            # Create a substitute execution unit
            substitute_unit: list[Job] = list()

            # Load the non empty jobs
            for job in unit:
                if type(job) != EmptyJob:
                    substitute_unit.append(job)

            # Run through fit_jobs list to fit lesser jobs 
            # in the substitute_unit
            rem_cores = max_binded_cores - min_binded_cores

            for cojob in fit_jobs:
                if self.cluster.half_node_cores(cojob) <= rem_cores:
                    self.cluster.waiting_queue.remove(cojob)
                    cojob.binded_cores = self.cluster.half_node_cores(cojob)
                    cojob.ratioed_remaining_time(substitute_unit[0])

                    substitute_unit.append(cojob)
                    rem_cores -= cojob.binded_cores

            # Redefine largest job speedup
            worst_job = min(substitute_unit[1:], key=(
                lambda job2: substitute_unit[0].get_speedup(job2)
            ))
            if substitute_unit[0].speedup != substitute_unit[0].get_speedup(worst_job):
                substitute_unit[0].ratioed_remaining_time(worst_job)
            
            # If rem_cores > 0 then there is an empty space left in the unit
            if rem_cores > 0:
                empty_job = EmptyJob(Job(None, -1, "empty", rem_cores, 
                                         None, None, None, rem_cores))
                substitute_unit.append(empty_job)

            # Remove the former unit from the execution list
            self.cluster.execution_list.remove(unit)

            # Recalculate ranks
            self.update_ranks()
            
            deploy_list.append(substitute_unit)

            # Calculate ll_avg_speedup
            ll_avg_speedup = (ll_avg_speedup * ll_num +
                              self.unit_avg_speedup(substitute_unit)) / (ll_num + 1)
            ll_num += 1

            # Write down event to logger
            self.logger.cluster_events["deploying:exec-colocation"] += 1


        return ll_num, ll_avg_speedup

    def deploying_wait_pairs(self, deploy_list, ll_num, ll_avg_speedup):

        ################################
        # Colocation with waiting jobs #
        ################################
        waiting_queue: list[Job] = deepcopy_list(self.cluster.waiting_queue)

        # Order waiting queue by needed cores starting with the lowest
        waiting_queue.sort(key=lambda job: self.wq_sort(job),
                           reverse=True)

        # Loop until waiting queue is empty
        while waiting_queue != []:

            # Get the job at the head
            job = self.pop(waiting_queue)
            
            # Create a dummy waiting queue without `job`
            wq = deepcopy_list(self.cluster.waiting_queue)
            wq.remove(job)

            # Filter out cojobs that can't fit into the execution list as pairs
            wq = list(filter(
                lambda cojob: 
                cojob.load.full_load_name in job.load.coloads and\
                2 * max(
                    self.cluster.half_node_cores(job),
                    self.cluster.half_node_cores(cojob)
                ) <= self.cluster.free_cores, wq
            ))

            # If empty, no pair can be made continue to the next job
            if wq == []:
                continue

            # Sort `wq` by the wait_ordering function
            wq.sort(key=(lambda x: self.coloc_ordering(job, x, ll_avg_speedup)),
                    reverse=True)

            execution_unit: list[Job] = [job]
            max_binded_cores = self.cluster.half_node_cores(job)
            
            # Build execution unit
            for cojob in wq:
                if self.cluster.half_node_cores(cojob) <= max_binded_cores:
                    waiting_queue.remove(cojob)
                    self.cluster.waiting_queue.remove(cojob)

                    cojob.ratioed_remaining_time(job)
                    cojob.binded_cores = self.cluster.half_node_cores(cojob)

                    execution_unit.append(cojob)

                    max_binded_cores -= cojob.binded_cores

            # If the other cojobs where larger
            # TODO: WARNING TEST OUT THE RATIOED REMAINING TIME
            if len(execution_unit) == 1:
                waiting_queue.append(job)
                continue
            
            self.cluster.waiting_queue.remove(job)
            
            # Set the speedup of the largest job
            worst_job = min(execution_unit[1:], key=(
                lambda cojob: execution_unit[0].get_speedup(cojob)
            ))

            execution_unit[0].ratioed_remaining_time(worst_job)
            execution_unit[0].binded_cores = self.cluster.half_node_cores(
                    execution_unit[0]
            )

            # If max_binded_cores is not 0 then fill the empty cores
            # with an empty job
            if max_binded_cores > 0:
                empty_job = EmptyJob(Job(None, -1, "empty", max_binded_cores, 
                                         None, None, None, max_binded_cores))
                execution_unit.append(empty_job)

            # Deploy them
            deploy_list.append(execution_unit)

            # Scheduler setup
            self.cluster.free_cores -= 2 * execution_unit[0].binded_cores
            ll_avg_speedup = (ll_avg_speedup * ll_num +
                              self.unit_avg_speedup(execution_unit)) / (ll_num + 1)
            ll_num += 1

            # Recalculate ranks
            self.update_ranks()

            # Logger cluster events update
            self.logger.cluster_events["deploying:wait-colocation"] += 1

            # Order waiting queue by needed cores starting with the lowest
            # waiting_queue.sort(key=lambda job: self.wq_sort(job, len(waiting_queue)),
            #                    reverse=True)

        return ll_num, ll_avg_speedup
