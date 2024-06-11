from abc import ABC, abstractmethod
import math

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
from procset import ProcSet
from typing import Protocol


class ScikitModel(Protocol):
    def predict(self, X):
        pass


class Coscheduler(Scheduler, ABC):
    """Base class for all co-scheduling algorithms
    """

    name = "Abstract Co-Scheduler"
    description = "Abstract base class for all co-scheduling algorithms"

    def __init__(self,
                 backfill_enabled: bool = False,
                 speedup_threshold: float = 1.0,
                 system_utilization: float = 1.0,
                 engine: Optional[ScikitModel] = None):

        Scheduler.__init__(self)

        self.backfill_enabled = backfill_enabled
        self.speedup_threshold = speedup_threshold
        self.system_utilization = system_utilization

        self.engine = engine
        self.heatmap: dict[str, dict] = dict()

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
                    self.heatmap[load.load_name].update({
                            co_load.load_name: self.engine.predict(
                                load.get_tag(), co_load.get_tag()
                            )
                    })

                    # Get speedup for co-load when co-scheduled with load
                    self.heatmap[co_load.load_name].update({
                            load.load_name: self.engine.predict(
                                co_load.get_tag(), load.get_tag()
                            )
                    })

                else:
                    # If we do not have an inference engine, then use the stored
                    # knowledge inside each load to get their speedups
                    # and if we do not have knowledge of their co-execution then
                    # submit a None value inside the heatmap

                    # Get speedup for load when co-scheduled with co-load
                    self.heatmap[load.load_name].update({

                            co_load.load_name:

                            load.get_med_speedup(co_load.load_name) 
                            if co_load.load_name in load.coscheduled_timelogs
                            else None

                    })

                    # Get speedup for co-load when co-scheduled with load
                    self.heatmap[co_load.load_name].update({

                            load.load_name:

                            co_load.get_med_speedup(load.load_name) 
                            if load.load_name in co_load.coscheduled_timelogs
                            else None

                    })

    @abstractmethod
    def waiting_job_candidates_reorder(self, job: Job, co_job: Job) -> float:
        pass

    @abstractmethod
    def xunit_candidates_reorder(self, job: Job, xunit: list[Job]) -> float:
        pass

    def after_deployment(self, *args):
        """After deploying a job in a pair or compact then some after processing
        events may need to take place. For example to calculate values necesary
        for the heuristics functions (e.g. the fragmentation of the cluster)
        """
        pass

    def best_xunit_candidate(self, job: Job) -> Optional[list[Job]]:
        """Return an executing unit (block of jobs) that is the best candidate
        for co-execution for a job. If no suitable xunit is found return None.

        + job: the job to find the best xunit candidate on its requirements
        """

        # We need to create a list of suitors for the job
        candidates: list[list[Job]] = list()

        # Get xunit candidates that satisfy the resources and speedup 
        # requirements
        for xunit in self.cluster.nonfilled_xunits():

            # Get the head job (largest job) and test its number of binded cores
            # (processors) with the idle job (job doing nothing) to see if the
            # jobs inside the xunit execute as spread or co-scheduled
            #
            # Spread means that the jobs are executing at top speedup while
            # co-scheduled means that their speedup is regulated by each other's
            # resource consumption
            #
            head_job: Job = xunit[0]
            idle_job: Job = xunit[-1]

            # Number of idle processors
            # idle_cores = idle_job.binded_cores
            idle_cores = len(idle_job.assigned_cores)

            # If idle cores are less than the resources the job wants to consume
            # then the xunit is not a candidate
            if job.half_node_cores > idle_cores:
                continue

            # If the job can fit then check if it will be co-allocated as the 
            # head job or as a tail job
            #if head_job.binded_cores >= idle_cores:
            if len(head_job.assigned_cores) >= idle_cores:
                # The job will be co-allocated as a tail job
                # We need to check whether the average speedup of the pairing
                # will be above the speedup_threshold
                if self.heatmap[head_job.job_name][job.job_name] is not None:
                    avg_speedup = (self.heatmap[job.job_name][head_job.job_name] + self.heatmap[head_job.job_name][job.job_name]) / 2.0
                else:
                    continue

                if avg_speedup > self.speedup_threshold:
                    candidates.append(xunit)
            else:
                # The job will be co-allocated as a head job
                # We need to check the average speedup with the worst possible
                # pairing with one of the worst jobs in the xunit
                worst_neighbor = min(xunit, 
                                     key=lambda neighbor: 
                                     self.heatmap[job.job_name][neighbor.job_name] 
                                     if type(neighbor) != EmptyJob and self.heatmap[job.job_name][neighbor.job_name] is not None 
                                     else math.inf)
                
                if self.heatmap[worst_neighbor.job_name][job.job_name] is not None:
                    avg_speedup = (self.heatmap[job.job_name][worst_neighbor.job_name] + self.heatmap[worst_neighbor.job_name][job.job_name]) / 2.0
                else:
                    continue

                if avg_speedup > self.speedup_threshold:
                    candidates.append(xunit)

        candidates.sort(key=lambda xunit: self.xunit_candidates_reorder(job, xunit), reverse=True)

        # If no candidates are found in the xunits return None
        if candidates == []:
            return None

        # Return best candidate for the job
        return candidates[0]

    def colocation_to_xunit(self, job: Job) -> bool:
        """Co-allocate job for execution in an already existing executing unit
        """

        # Get the best candidate for the job
        best_candidate = self.best_xunit_candidate(job)

        # If no candidate is found then return
        if best_candidate is None:
            return False

        # Else a candidate was found

        # Setup the job and the queues
        self.cluster.waiting_queue.remove(job)
        job.start_time = self.cluster.makespan
        job.binded_cores = job.half_node_cores

        # Check if it will be as a head or tail job
        head_job = best_candidate[0]
        idle_job = best_candidate[-1]
        # Remove idle job because it will be replaced
        best_candidate.remove(idle_job)

        # It will be a tail job
        # if head_job.binded_cores >= idle_job.binded_cores:
        if len(head_job.assigned_cores) >= len(idle_job.assigned_cores):

            # Calculate the remaining time of the job and the new speedup
            job.ratioed_remaining_time(head_job)

            # If the job produces worse speedup for the current head job of the
            # xunit then re-calculate the head job's remaining time and speedup
            if self.heatmap[head_job.job_name][job.job_name] < head_job.speedup:
                head_job.ratioed_remaining_time(job)

            # Add the job to the best xunit candidate
            best_candidate.append(job)

        # It will be a head job
        else:

            # Recalculate remaining time and speedup for each job inside the 
            # executing unit
            for xjob in best_candidate:
                xjob.ratioed_remaining_time(job)

            # Find the worst neighbor for the job
            worst_neighbor = min(best_candidate, 
                                 key=lambda neighbor: 
                                 self.heatmap[job.job_name][neighbor.job_name] 
                                 if type(neighbor) != EmptyJob and self.heatmap[job.job_name][neighbor.job_name] is not None 
                                 else math.inf)
            job.ratioed_remaining_time(worst_neighbor)

            best_candidate.insert(0, job)

        # Assign processors to the job
        half_node_cores = int(self.cluster.cores_per_node / 2)
        # Job's required number of cores/processors
        job_req_cores = job.half_node_cores

        # Create a list of processors for assignment
        job_to_bind_procs = list()

        # Find the processors to be assigned sequentially inside the remaining
        # available processors of the xunit
        # idle_job is responsible to keep track of the idle processors in a
        # xunit
        for procint in idle_job.assigned_cores.intervals():

            # If the interval is equal to a half socket then get the whole half
            # socket
            if len(procint) == half_node_cores:
                job_to_bind_procs.append(f"{procint.inf}-{procint.sup}")
                job_req_cores -= half_node_cores

            # If the interval has at least more than half socket then jump at
            # each half socket
            elif len(procint) > half_node_cores:

                # A list of the available processors inside the interval
                interval = list(range(procint.inf, procint.sup + 1))

                # Required number of nodes (jumps) for the remaining requested
                # cores of the job
                req_nodes = int(job_req_cores / half_node_cores)

                # Number of available nodes inside the xunit
                avail_nodes = int(len(procint) / (2 * half_node_cores))

                # If we need more nodes than the ones provided we consume them
                # and move on to the next interval of idle processors
                if req_nodes > avail_nodes:
                    i = 0
                    for _ in range(avail_nodes):
                        job_to_bind_procs.extend([
                            str(processor)
                            for processor in interval[i:i+half_node_cores]
                        ])
                        i += 2 * half_node_cores
                        job_req_cores -= half_node_cores
                # If the requirements are met then we consume only the number of
                # nodes needed for the job
                else:
                    i = 0
                    for _ in range(req_nodes):
                        job_to_bind_procs.extend([
                            str(processor)
                            for processor in interval[i:i+half_node_cores]
                        ])
                        i += 2 * half_node_cores
                        job_req_cores -= half_node_cores

            else:
                # Should not appear as a choice
                continue

            if job_req_cores == 0:
                break

        job.assigned_cores = ProcSet.from_str(" ".join(job_to_bind_procs))

        #if idle_job.binded_cores > job.binded_cores:
        if len(idle_job.assigned_cores) > len(job.assigned_cores):
            best_candidate.append(EmptyJob(Job(
                None, 
                -1, 
                "idle", 
                idle_job.binded_cores - job.binded_cores,
                idle_job.binded_cores - job.binded_cores,
                idle_job.assigned_cores - job.assigned_cores,
                -1, 
                -1,
                None, 
                None, 
                None, 
                None
            )))
        
        # It was deployed to an xunit
        return True

    def best_wjob_candidates(self, job: Job, waiting_queue_slice: list[Job]) -> Optional[Job]:

        candidates: list[Job] = list()

        for wjob in waiting_queue_slice:

            # The speedup values must exist
            conditions  = self.heatmap[job.job_name][wjob.job_name] is not None
            if not conditions:
                continue
            # The pair must fit in the remaining free processors of the cluster
            # conditions &= 2 * max(job.half_node_cores, wjob.half_node_cores) <= len(self.cluster.total_procs)
            conditions &= self.assign_nodes(
                    2 * max(job.half_node_cores, wjob.half_node_cores), 
                    self.cluster.total_procs) is not None
            # The pair's average speedup must be higher than the user defined
            # speedup threshold
            conditions &= (self.heatmap[job.job_name][wjob.job_name] + self.heatmap[wjob.job_name][job.job_name]) / 2.0 > self.speedup_threshold

            # If it satisfies all the conditions then it is a candidate pairing job
            if conditions:
                candidates.append(wjob)

        candidates.sort(key=lambda wjob: self.waiting_job_candidates_reorder(job, wjob), reverse=True)

        # If no candidates were found return None
        if candidates == []:
            return None

        # Return best candidate for job
        return candidates[0]

    def colocation_with_wjobs(self, job: Job, waiting_queue_slice: list[Job]) -> bool:
        """Co-allocate two waiting jobs to create a new executing unit
        """
        
        # Check if there is any hope for the job to reduce some CPU cycles
        if 2 * job.half_node_cores > len(self.cluster.total_procs):
            return False

        best_candidate = self.best_wjob_candidates(job, waiting_queue_slice)

        if best_candidate is None:
            # IDEA: spread execution here?
            # If there isn't any candidate (yet) for the job then try spread
            system_utilization = 1 - len(self.cluster.total_procs) / (self.cluster.nodes * self.cluster.cores_per_node)
            if system_utilization <= self.system_utilization:
                procset = self.assign_nodes(2 * job.half_node_cores, self.cluster.total_procs)
                if procset is not None:

                    # Create a spread xunit
                    new_xunit: list[Job] = list()

                    self.cluster.total_procs -= procset
                    self.cluster.waiting_queue.remove(job)
                    job.start_time = self.cluster.makespan
                    job.remaining_time *= (1 / job.get_max_speedup())
                    job.speedup = job.get_max_speedup()

                    # Allocate cores to job
                    half_node_cores = int(self.cluster.cores_per_node / 2)
                    job_req_cores = job.half_node_cores
                    job_to_bind_procs = []
                    i = 0
                    while job_req_cores >= half_node_cores:
                        job_to_bind_procs.extend(procset[i:i+half_node_cores])
                        job_req_cores -= half_node_cores
                        i += 2 * half_node_cores

                    if job_req_cores != 0:
                        job_to_bind_procs.extend(procset[i:i+job_req_cores])

                    job.assigned_cores = ProcSet.from_str(" ".join(
                        [str(processor) for processor in job_to_bind_procs]
                    ))
                    procset -= job.assigned_cores

                    idle_job = EmptyJob(Job(
                        None, 
                        -1, 
                        "idle", 
                        -1, 
                        -1, 
                        procset, 
                        -1, 
                        -1,
                        None, 
                        None, 
                        None, 
                        None
                    ))

                    new_xunit.extend([job, idle_job])
                    self.cluster.execution_list.append(new_xunit)
                    return True

            return False

        new_xunit: list[Job] = list()

        # Remove the jobs from the original waiting queue
        self.cluster.waiting_queue.remove(job)
        self.cluster.waiting_queue.remove(best_candidate)

        # Remove job from the copy of the waiting queue to avoid
        # double allocations
        waiting_queue_slice.remove(best_candidate)

        job.start_time = self.cluster.makespan
        best_candidate.start_time = self.cluster.makespan

        best_candidate.ratioed_remaining_time(job)
        job.ratioed_remaining_time(best_candidate)

        total_required_cores = 2 * max(job.half_node_cores, best_candidate.half_node_cores)

        procset = self.assign_nodes(total_required_cores, self.cluster.total_procs)
        if procset is None:
            raise RuntimeError("The procset must not be empty for a chosen candidate")

        self.cluster.total_procs -= procset
        half_node_cores = int(self.cluster.cores_per_node / 2)

        if best_candidate.half_node_cores > job.half_node_cores:

            # Best candidate will be the head job
            new_xunit.append(best_candidate)
            new_xunit.append(job)

            # Assigning processors to best candidate
            best_candidate_req_cores = best_candidate.half_node_cores
            best_candidate_to_bind_procs = []
            i = 0
            while best_candidate_req_cores > half_node_cores:
                best_candidate_to_bind_procs.extend(procset[i:i+half_node_cores])
                best_candidate_req_cores -= half_node_cores
                i += 2 * half_node_cores

            if best_candidate_req_cores != 0:
                best_candidate_to_bind_procs.extend(procset[i:i+best_candidate_req_cores])

            best_candidate.assigned_cores = ProcSet.from_str(" ".join(
                [str(processor) for processor in best_candidate_to_bind_procs]
            ))
            procset -= best_candidate.assigned_cores

            # Assigning processors to job
            job.assigned_cores = ProcSet.from_str(" ".join([
                str(processor) for processor in procset[:job.half_node_cores]
            ]))
            procset -= job.assigned_cores

            idle_cores = best_candidate.half_node_cores - job.half_node_cores
            new_xunit.append(EmptyJob(Job(
                None, 
                -1, 
                "idle", 
                idle_cores, 
                idle_cores,
                procset,
                -1, 
                -1,
                None, 
                None, 
                None, 
                None
            )))

        else:

            # Job will be the head job
            new_xunit.append(job)
            new_xunit.append(best_candidate)

            # Assigning processors to job
            job_req_cores = job.half_node_cores
            job_to_bind_procs = []
            i = 0
            while job_req_cores > half_node_cores:
                job_to_bind_procs.extend(procset[i:i+half_node_cores])
                job_req_cores -= half_node_cores
                i += 2 * half_node_cores

            if job_req_cores != 0:
                job_to_bind_procs.extend(procset[i:i+job_req_cores])

            job.assigned_cores = ProcSet.from_str(" ".join(
                [str(processor) for processor in job_to_bind_procs]
            ))
            procset -= job.assigned_cores
                
            # Assigning processors to best candidate
            best_candidate.assigned_cores = ProcSet.from_str(" ".join(
                [
                    str(processor) 
                    for processor in procset[:best_candidate.half_node_cores]
                ]
            ))
            procset -= best_candidate.assigned_cores

            idle_cores = job.half_node_cores - best_candidate.half_node_cores

            if idle_cores > 0:
                new_xunit.append(EmptyJob(Job(
                    None, 
                    -1, 
                    "idle", 
                    idle_cores, 
                    idle_cores,
                    procset,
                    -1, 
                    -1,
                    None, 
                    None, 
                    None, 
                    None
                )))

        self.cluster.execution_list.append(new_xunit)
        return True

    @abstractmethod
    def deploy(self) -> None:
        pass
