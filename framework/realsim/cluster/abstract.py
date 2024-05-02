import abc
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from realsim.jobs import Job, EmptyJob
from realsim.jobs.utils import deepcopy_list
from realsim.scheduler.scheduler import Scheduler
from realsim.logger.logger import Logger

import math


class AbstractCluster(abc.ABC):

    def __init__(self, nodes, cores_per_node):

        # Number of nodes
        self.nodes = nodes
        # Number of cores per node
        self.cores_per_node = cores_per_node
        # Number of total cores
        self.total_cores = self.nodes * self.cores_per_node
        # Number of current free cores
        self.free_cores = self.total_cores

        # Scheduler instance for the cluster
        self.scheduler: Scheduler

        # Logger instance for the cluster
        self.logger: Logger

        # Loaded jobs to pre-waiting queue based on their queued time
        self.preloaded_queue: list[Job] = list()
        # The generated jobs are first deployed here
        self.waiting_queue: list[Job] = list()
        # The queue of jobs that are executing
        self.execution_list: list[list[Job]] = list()
        # Finished jobs' ids list
        self.finished_jobs: list[int] = list()

        # Important counters #

        # Job id counter
        self.id_counter: int = 0
        
        # The total execution time
        # of a cluster
        self.makespan: float = 0

    def assign_scheduler(self, scheduler: Scheduler):
        self.scheduler = scheduler
        self.scheduler.assign_cluster(self)

    def assign_logger(self, logger: Logger):
        self.logger = logger
        self.logger.assign_cluster(self)

    def half_node_cores(self, job: Job) -> int:
        return job.half_node_cores

    def full_node_cores(self, job: Job) -> int:
        return job.full_node_cores

    def preload_jobs(self, jobs_set: list[Job]) -> None:
        # Get a clean deep copy of the set of jobs
        copy = deepcopy_list(jobs_set)

        # Sort jobs by their time they appear on the waiting queue
        copy.sort(key=lambda job: job.submit_time)

        if self.makespan == 0:
            self.id_counter = 0

        # Preload jobs and calculate their respective half and full node cores
        # usage
        for job in copy:
            job.job_id = self.id_counter
            job.half_node_cores = int(math.ceil(job.num_of_processes / (self.cores_per_node / 2)) * (self.cores_per_node / 2))
            job.full_node_cores = int(math.ceil(job.num_of_processes / self.cores_per_node) * self.cores_per_node)
            self.id_counter += 1
            self.preloaded_queue.append(job)

    def load_in_waiting_queue(self) -> None:

        copy = deepcopy_list(self.preloaded_queue)

        for job in copy:
            if job.submit_time <= self.makespan:
                self.waiting_queue.append(job)
                self.preloaded_queue.remove(job)

    def filled_xunits(self) -> list[list[Job]]:
        """Return all the executing units that have no empty space. All the
        binded cores are completely filled.
        """

        filled_units: list[list[Job]] = list()

        for unit in self.execution_list:

            filled = True

            for job in unit:
                if type(job) == EmptyJob:
                    filled = False
                    break

            if filled:
                filled_units.append(unit)

        return filled_units

    def nonfilled_xunits(self) -> list[list[Job]]:
        """Return all the execution units that have empty space. All the
        binded cores are not filled.
        """

        nonfilled_units: list[list[Job]] = list()

        for execution_unit in self.execution_list:

            # We don't care about compact jobs
            if len(execution_unit) == 1:
                continue

            # At least one has to be non empty and at least one empty
            non_empty = 0
            empty = 0
            for job in execution_unit:
                if type(job) == EmptyJob:
                    empty += 1
                    break
                else:
                    non_empty += 1

            if non_empty > 0 and empty > 0:
                nonfilled_units.append(execution_unit)

        return nonfilled_units

    @abc.abstractmethod
    def next_state(self) -> None:
        pass

    @abc.abstractmethod
    def free_resources(self) -> None:
        pass

    def setup(self):
        self.free_cores = self.total_cores
        self.makespan = 0
        self.execution_list = list()

    def step(self):

        # This is definite
        # If broken then the simulation loop has problems
        if self.free_cores < 0 or self.free_cores > self.total_cores:
            raise RuntimeError(f"Free cores: {self.free_cores}")

        # Deploy to waiting queue any preloaded jobs that remain
        self.load_in_waiting_queue()
        
        # Check if there are any jobs left waiting
        if self.waiting_queue != []:

            # Deploy/Submit jobs to the execution list
            self.scheduler.deploy()

            # If scheduler deployed jobs to execution list successfully and the
            # backfilling policy is enabled
            if self.scheduler.backfill_enabled:

                # Execute the backfilling algorithm
                self.scheduler.backfill()

        self.logger.evt_jobs_executing()

        # If the scheduler didn't deploy jobs then
        # the execution list is full and we have to
        # execute some
        self.next_state()

        if self.execution_list != []:
            # Free the resources
            self.free_resources()
