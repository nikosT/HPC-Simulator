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
    def waiting_job_candidates_reorder(self, job: Job, co_job: Job) -> float:
        pass

    @abstractmethod
    def xunit_candidates_reorder(self, job: Job, xunit: list[Job]) -> float:
        pass

    def after_deployment(self, *args):
        """After deployment work to be done
        """
        pass

    def best_xunit_candidate(self, job: Job) -> Optional[list[Job]]:

        candidates: list[list[Job]] = list()

        # Get xunit candidates that satisfy the resources and speedup 
        # requirements
        for xunit in self.cluster.nonfilled_xunits():

            # Get head job and test it with empty space to see if it is at
            # spread or co-allocation executing state
            head_job = xunit[0]
            idle_job = xunit[-1]

            idle_cores = idle_job.binded_cores

            # If idle cores are less than the resources the job wants to consume
            # then the xunit is not a candidate
            if job.binded_cores > idle_cores:
                continue

            # If the job can fit then check if it will be co-allocated as the 
            # head job or as a tail job
            if head_job.binded_cores >= idle_cores:
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

        best_candidate = self.best_xunit_candidate(job)

        if best_candidate is None:
            # It failed to secure a best candidate
            return False

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
        if head_job.binded_cores >= idle_job.binded_cores:

            job.ratioed_remaining_time(head_job)
            if self.heatmap[head_job.job_name][job.job_name] < head_job.speedup:
                head_job.ratioed_remaining_time(job)

            best_candidate.append(job)

        # It will be a head job
        else:

            # Recalculate remaining time and speedup for each job inside the 
            # executing unit
            for xjob in best_candidate:
                xjob.ratioed_remaining_time(job)

            # Find the worst neighbor for the job
            #worst_neighbor = min(best_candidate, key=lambda neighbor: self.heatmap[job.job_name][neighbor.job_name])
            worst_neighbor = min(best_candidate, 
                                 key=lambda neighbor: 
                                 self.heatmap[job.job_name][neighbor.job_name] 
                                 if type(neighbor) != EmptyJob and self.heatmap[job.job_name][neighbor.job_name] is not None 
                                 else math.inf)
            job.ratioed_remaining_time(worst_neighbor)

            best_candidate.insert(0, job)

        half_node_cores = int(self.cluster.cores_per_node / 2)
        job_req_cores = job.half_node_cores
        job_to_bind_procs = list()
        for procint in idle_job.assigned_procs.intervals():

            # If the interval is equal to a half socket
            if len(procint) == half_node_cores:
                job_to_bind_procs.append(f"{procint.inf}-{procint.sup}")
                job_req_cores -= half_node_cores

            # If the interval has at least more than half socket then step at
            # each half socket
            elif len(procint) > half_node_cores:

                interval = list(range(procint.inf, procint.sup + 1))
                req_nodes = int(job_req_cores / half_node_cores)
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

        job.assigned_procs = ProcSet.from_str(" ".join(job_to_bind_procs))

        if idle_job.binded_cores > job.binded_cores:
            best_candidate.append(EmptyJob(Job(
                None, 
                -1, 
                "idle", 
                idle_job.binded_cores - job.binded_cores,
                idle_job.binded_cores - job.binded_cores,
                idle_job.assigned_procs - job.assigned_procs,
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

            conditions  = self.heatmap[job.job_name][wjob.job_name] is not None
            if not conditions:
                continue
            conditions &= 2 * max(job.half_node_cores, wjob.half_node_cores) <= self.cluster.free_cores
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

        best_candidate = self.best_wjob_candidates(job, waiting_queue_slice)

        if best_candidate is None:
            # IDEA: spread execution here?
            return False

        new_xunit: list[Job] = list()

        # Remove the jobs from the original waiting queue
        self.cluster.waiting_queue.remove(job)
        self.cluster.waiting_queue.remove(best_candidate)

        # Remove job from the copy of the waiting queue to avoid
        # double allocations
        waiting_queue_slice.remove(best_candidate)

        best_candidate.binded_cores = best_candidate.half_node_cores
        job.binded_cores = job.half_node_cores

        job.start_time = self.cluster.makespan
        best_candidate.start_time = self.cluster.makespan

        best_candidate.ratioed_remaining_time(job)
        job.ratioed_remaining_time(best_candidate)

        total_required_cores = 2 * max(job.binded_cores, best_candidate.binded_cores)
        self.cluster.free_cores -= total_required_cores

        procset = self.assign_procs(total_required_cores)
        self.cluster.total_procs -= procset
        half_node_cores = int(self.cluster.cores_per_node / 2)

        if best_candidate.binded_cores > job.binded_cores:

            # Best candidate will be the head job
            new_xunit.append(best_candidate)
            new_xunit.append(job)

            # Assigning processors to best candidate
            best_candidate_req_cores = best_candidate.binded_cores
            best_candidate_to_bind_procs = []
            i = 0
            while best_candidate_req_cores > half_node_cores:
                best_candidate_to_bind_procs.extend(procset[i:i+half_node_cores])
                best_candidate_req_cores -= half_node_cores
                i += 2 * half_node_cores

            if best_candidate_req_cores != 0:
                best_candidate_to_bind_procs.extend(procset[i:i+best_candidate_req_cores])

            best_candidate.assigned_procs = ProcSet.from_str(" ".join(
                [str(processor) for processor in best_candidate_to_bind_procs]
            ))
            procset -= best_candidate.assigned_procs

            # Assigning processors to job
            job.assigned_procs = ProcSet.from_str(" ".join([
                str(processor) for processor in procset[:job.binded_cores]
            ]))
            procset -= job.assigned_procs

            idle_cores = best_candidate.binded_cores - job.binded_cores
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
            job_req_cores = job.binded_cores
            job_to_bind_procs = []
            i = 0
            while job_req_cores > half_node_cores:
                job_to_bind_procs.extend(procset[i:i+half_node_cores])
                job_req_cores -= half_node_cores
                i += 2 * half_node_cores

            if job_req_cores != 0:
                job_to_bind_procs.extend(procset[i:i+job_req_cores])

            job.assigned_procs = ProcSet.from_str(" ".join(
                [str(processor) for processor in job_to_bind_procs]
            ))
            procset -= job.assigned_procs
                
            # Assigning processors to best candidate
            best_candidate.assigned_procs = ProcSet.from_str(" ".join(
                [
                    str(processor) 
                    for processor in procset[:best_candidate.binded_cores]
                ]
            ))
            procset -= best_candidate.assigned_procs

            idle_cores = job.binded_cores - best_candidate.binded_cores

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
