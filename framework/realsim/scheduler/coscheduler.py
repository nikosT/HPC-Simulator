from abc import ABC, abstractmethod

import os
import sys
from typing import Optional

sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), '../../')
))

sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), '../../../')
))

from api.loader import Load
from realsim.scheduler.scheduler import Scheduler
from realsim.jobs import Job, EmptyJob
from realsim.jobs.utils import deepcopy_list
from numpy import average as avg
from typing import Protocol


class ScikitModel(Protocol):
    def predict(self, X):
        pass


class Coscheduler(Scheduler, ABC):
    """We try to provide at every checkpoint an execution list whose average
    speedup is higher than 1. We try to distribute the higher speedup candidates
    among the checkpoints.
    """

    name = "Abstract Co-Scheduler"
    description = "Exhaustive coupling co-scheduling base class for ExhaustiveCluster"

    def __init__(self, 
                 threshold: float = 1, 
                 system_utilization: float = 1,
                 engine: Optional[ScikitModel] = None):
        self.threshold = threshold
        self.system_utilization = system_utilization
        self.engine = engine
        self.heatmap: dict[str, dict] = dict()
        Scheduler.__init__(self)

    def setup(self):
        """Create the heatmap for the jobs in the waiting queue
        """

        # Initialize the heatmap
        for job in self.cluster.preloaded_queue:
            self.heatmap[job.job_name] = {}

        # Get a copy of the preloaded queue
        preloaded_queue = deepcopy_list(self.cluster.preloaded_queue)

        while preloaded_queue != []:

            job: Job = self.pop(preloaded_queue)

            load: Optional[Load] = job.load

            if load is None:
                raise RuntimeError("A job with an empty load was found inside the waiting queue at the startup stage")

            for co_job in preloaded_queue:

                co_load: Optional[Load] = co_job.load

                if co_load is None:
                    raise RuntimeError("A job with an empty load was found inside the waiting queue at the startup stage")

                if self.engine is not None:
                    # If an inference engine is provided then predict the
                    # speedup for both load and co-load when co-scheduled

                    # Get speedup for load when co-scheduled with co-load
                    self.heatmap[load.full_load_name].update({
                            co_load.full_load_name: self.engine.predict(
                                load.get_tag(), co_load.get_tag()
                            )
                    })

                    # Get speedup for co-load when co-scheduled with load
                    self.heatmap[co_load.full_load_name].update({
                            load.full_load_name: self.engine.predict(
                                co_load.get_tag(), load.get_tag()
                            )
                    })

                else:
                    # If we do not have an inference engine, then use the stored
                    # knowledge inside each load to get their speedups
                    # and if we do not have knowledge of their co-execution then
                    # submit a None value inside the heatmap

                    # Get speedup for load when co-scheduled with co-load
                    self.heatmap[load.full_load_name].update({

                            co_load.full_load_name:

                            load.get_median_speedup(co_load.full_load_name) 
                            if co_load.full_load_name in load.coloads
                            else None

                    })

                    # Get speedup for co-load when co-scheduled with load
                    self.heatmap[co_load.full_load_name].update({

                            load.full_load_name:

                            co_load.get_median_speedup(load.full_load_name) 
                            if load.full_load_name in co_load.coloads
                            else None

                    })

    @abstractmethod
    def xunits_order(self, xunit: list[Job]) -> float:
        pass

    @abstractmethod
    def xunits_candidates_order(self, largest_job: Job, job: Job) -> float:
        pass

    @abstractmethod
    def waiting_queue_order(self, job: Job) -> float:
        pass

    @abstractmethod
    def wjob_candidates_order(self, job: Job, co_job: Job) -> float:
        pass

    def after_deployment(self, xunit: list[Job]):
        """After deployment work to be done
        """
        pass

    def xunit_candidates(self, 
                         largest_job: Job, 
                         empty_space: int) -> list[Job]:

        # Get candidates that have a speedup and they fit inside the xunit
        candidates = list(
                filter(
                    lambda job:

                    self.heatmap[largest_job.job_name][job.job_name] is not None 
                    and
                    job.half_node_cores <= empty_space,

                    self.cluster.waiting_queue
                )
        )

        # Filter candidates whose average speedup is greater than the threshold
        candidates = list(filter(
            lambda co_job: 
            avg([
                self.heatmap[largest_job.job_name][co_job.job_name],
                self.heatmap[co_job.job_name][largest_job.job_name]
                ]) > self.threshold,
            candidates
        ))

        # Sort candidate jobs by an ordering function
        candidates.sort(
                key=(
                    lambda co_job: 
                    self.xunits_candidates_order(largest_job, co_job)
                    ),
                reverse=True
        )

        return candidates

    def deploying_to_xunits(self, deploying_list):
        """Deploying waiting jobs to executing units that have available free
        space
        """

        nonfilled_xunits = self.cluster.nonfilled_xunits()

        nonfilled_xunits.sort(
                key=lambda unit: self.xunits_order(unit),
                reverse=True
        )

        for xunit in nonfilled_xunits:

            largest_job = xunit[0]

            # Get the largest job's binded cores
            max_binded_cores = largest_job.binded_cores

            # Get the sum of binded cores of the smallest jobs
            min_binded_cores = 0
            for job in xunit[1:]:
                if type(job) != EmptyJob:
                    min_binded_cores += job.binded_cores

            # Empty space inside the xunit
            empty_space = max_binded_cores - min_binded_cores

            # Get the possible candidates for the xunit
            candidates = self.xunit_candidates(largest_job, empty_space)

            # If no candidate was found then check if there is only one non
            # empty job inside the unit
            if candidates == []:

                # If no job is co-executing with the largest job inside the
                # xunit then change the execution policy to spread
                if min_binded_cores == 0: 

                    # Check if spread and if not change it to spread
                    if largest_job.speedup != largest_job.get_max_speedup():
                        largest_job.remaining_time *= largest_job.speedup / largest_job.get_max_speedup()
                        largest_job.speedup = largest_job.get_max_speedup()

                    empty_job = EmptyJob(Job(None, 
                                             -1, 
                                             "empty", 
                                             empty_space, 
                                             empty_space,
                                             -1,
                                             -1,
                                             None, 
                                             None, 
                                             None, 
                                             None))

                    new_xunit = [largest_job, empty_job]

                    # Deployment!
                    deploying_list.append(new_xunit)
                    self.after_deployment(new_xunit)

                else:

                    # If there are other jobs except the largest that are still
                    # executing then find the job that minimizes the largest's
                    # job speedup
                    neighbors = list(filter(
                        lambda job: type(job) != EmptyJob,
                        xunit[1:]
                    ))

                    worst_neighbor = min(neighbors, 
                                         key=lambda job: largest_job.get_speedup(job)
                                         )

                    # If the worst neighbor hasn't finished executing
                    if largest_job.speedup != largest_job.get_speedup(worst_neighbor):
                        largest_job.ratioed_remaining_time(worst_neighbor)

                    # Deployment!
                    deploying_list.append(xunit)
                    self.after_deployment(xunit)

                continue

            # If there are candidates then substitute the xunit with a new one
            # that includes as much possible candidates

            # Create a substitute execution unit
            substitute_unit: list[Job] = list()

            # Load the non empty jobs
            for job in xunit:
                if type(job) != EmptyJob:
                    substitute_unit.append(job)

            # Run through fit_jobs list to fit lesser jobs 
            # in the substitute_unit
            rem_cores = empty_space

            for co_job in candidates:

                if co_job.half_node_cores <= rem_cores:

                    self.cluster.waiting_queue.remove(co_job)
                    co_job.binded_cores = co_job.half_node_cores
                    co_job.ratioed_remaining_time(substitute_unit[0])

                    substitute_unit.append(co_job)
                    rem_cores -= co_job.binded_cores

            # Redefine largest job speedup
            worst_neighbor = min(substitute_unit[1:], key=(
                lambda co_job: substitute_unit[0].get_speedup(co_job)
            ))

            if substitute_unit[0].speedup != substitute_unit[0].get_speedup(worst_neighbor):
                substitute_unit[0].ratioed_remaining_time(worst_neighbor)
            
            # If rem_cores > 0 then there is an empty space left in the unit
            if rem_cores > 0:
                empty_job = EmptyJob(Job(None, 
                                         -1,
                                         "empty",
                                         rem_cores,
                                         rem_cores, 
                                         -1,
                                         -1,
                                         None, 
                                         None, 
                                         None, 
                                         None))
                substitute_unit.append(empty_job)

            # Remove the former unit from the execution list
            self.cluster.execution_list.remove(xunit)
            
            # Deployment!
            deploying_list.append(substitute_unit)

            self.deploying = True
            self.after_deployment(substitute_unit)

            # Write down event to logger
            self.logger.cluster_events["deploying:exec-colocation"] += 1

        return

    def wjob_candidates(self, 
                        job: Job, 
                        candidates: list[Job]) -> list[Job]:

        # Filter out cojobs that can't fit into the execution list as pairs
        candidates = list(filter(
            lambda co_job: 

            self.heatmap[co_job.job_name][job.job_name] is not None 
            and
            2 * max(
                job.half_node_cores,
                co_job.half_node_cores
            ) <= self.cluster.free_cores, 

            candidates
        ))

        candidates = list(filter(
            lambda co_job:

            avg([
                self.heatmap[job.job_name][co_job.job_name],
                self.heatmap[co_job.job_name][job.job_name]
                ]) > self.threshold,

            candidates
        ))

        # Sort `wq` by the wait_ordering function
        candidates.sort(key=lambda co_job: 
                        self.wjob_candidates_order(job, co_job),
                        reverse=True)

        return candidates

    def deploying_wait_pairs(self, deploying_list):

        ################################
        # Colocation with waiting jobs #
        ################################
        waiting_queue: list[Job] = deepcopy_list(self.cluster.waiting_queue)

        # Order waiting queue by needed cores starting with the lowest
        waiting_queue.sort(key=lambda job: self.waiting_queue_order(job),
                           reverse=True)

        # Loop until waiting queue is empty
        while waiting_queue != []:

            # Get the job at the head
            job = self.pop(waiting_queue)
            
            # Create a dummy waiting queue without `job`
            candidates = deepcopy_list(self.cluster.waiting_queue)
            candidates.remove(job)

            candidates = self.wjob_candidates(job, candidates)

            # If empty, no pair can be made continue to the next job
            if candidates == []:
                continue

            # If there are candidates then create a new xunit
            xunit: list[Job] = [job]
            max_binded_cores = job.half_node_cores
            
            # Build execution unit
            for co_job in candidates:
                if co_job.half_node_cores <= max_binded_cores:

                    waiting_queue.remove(co_job)
                    self.cluster.waiting_queue.remove(co_job)

                    co_job.ratioed_remaining_time(job)
                    co_job.binded_cores = co_job.half_node_cores

                    xunit.append(co_job)

                    max_binded_cores -= co_job.binded_cores

            # If the other cojobs where larger
            # TODO: WARNING TEST OUT THE RATIOED REMAINING TIME
            if len(xunit) == 1:

                waiting_queue.append(job)
                
                continue
            
            self.cluster.waiting_queue.remove(job)
            
            # Set the speedup of the largest job
            worst_neighbor = min(xunit[1:], key=(
                lambda co_job: job.get_speedup(co_job)
            ))

            job.ratioed_remaining_time(worst_neighbor)
            job.binded_cores = job.half_node_cores

            # If max_binded_cores is not 0 then fill the empty cores
            # with an empty job
            if max_binded_cores > 0:
                empty_job = EmptyJob(
                        Job(None, 
                            -1, 
                            "empty", 
                            max_binded_cores, 
                            max_binded_cores,
                            -1,
                            -1,
                            None, 
                            None, 
                            None, 
                            None))

                xunit.append(empty_job)

            # Deployment!
            deploying_list.append(xunit)

            # Scheduler setup
            self.deploying = True
            self.after_deployment(xunit)

            # Cluster setup
            self.cluster.free_cores -= 2 * job.binded_cores

            # Logger cluster events update
            self.logger.cluster_events["deploying:wait-colocation"] += 1

        return

    def deploying_wait_compact(self, deploying_list):

        #############################
        # Compact with waiting jobs #
        #############################
        waiting_queue = deepcopy_list(self.cluster.waiting_queue)
        
        # Order by the least amount of needed cores
        # waiting_queue.sort(key=(lambda job: job.num_of_processes))

        while waiting_queue != []:

            job = self.pop(waiting_queue)

            if job.full_node_cores <= int(self.cluster.free_cores):

                # Remove from cluster's waiting queue
                self.cluster.waiting_queue.remove(job)

                # Setup job
                job.binded_cores = job.full_node_cores

                # Deploy job
                deploying_list.append([job])

                # Cluster setup
                self.cluster.free_cores -= job.binded_cores

                # Scheduler setup
                self.deploying = True
                self.after_deployment([job])

                # Logger cluster events update
                self.logger.cluster_events["deploying:compact"] += 1

        return

    def deploying_as_spread(self, deploying_list):

        # Get a copy of the current waiting queue of the cluster
        waiting_queue: list[Job] = deepcopy_list(self.cluster.waiting_queue)

        # Order waiting queue by needed cores starting with the lowest
        waiting_queue.sort(key=lambda job: self.waiting_queue_order(job),
                           reverse=True)

        while waiting_queue != []:

            # Get first job
            job = self.pop(waiting_queue)

            condition = (2 * job.half_node_cores) <= self.cluster.free_cores
            condition &= job.get_max_speedup() > self.threshold
            condition &= (self.cluster.free_cores / self.cluster.total_cores) > self.system_utilization

            # If the job fits and the cores utilization of the system meets the
            # requirements specified then submit as spread
            if condition:

                self.cluster.waiting_queue.remove(job)
                job.binded_cores = job.half_node_cores
                job.speedup = job.get_max_speedup()

                # Deploying job
                empty_space = EmptyJob(Job(None, 
                                           -1, 
                                           "empty",
                                           job.binded_cores,
                                           job.binded_cores,
                                           -1,
                                           -1,
                                           None,
                                           None,
                                           None,
                                           None))
                xunit = [job, empty_space]
                deploying_list.append(xunit)

                self.cluster.free_cores -= 2 * job.binded_cores

                self.deploying = True
                self.after_deployment(xunit)

                # Logger cluster events update
                self.logger.cluster_events["deploying:spread"] += 1

        return

    @abstractmethod
    def deploy(self) -> bool:
        pass
