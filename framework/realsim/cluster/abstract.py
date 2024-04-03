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

        # The generated jobs are first deployed here
        self.waiting_queue: list[Job] = list()
        # The queue of jobs that are executing
        self.execution_list: list[list[Job]] = list()
        # Finished jobs' ids list
        self.finished_jobs: list[int] = list()

        # Important counters #

        # Job id counter
        self.id_counter = 0
        
        # The total execution time
        # of a cluster
        self.makespan = 0

    def assign_scheduler(self, scheduler: Scheduler):
        self.scheduler = scheduler
        self.scheduler.assign_cluster(self)

    def assign_logger(self, logger: Logger):
        self.logger = logger
        self.logger.assign_cluster(self)

    def half_node_cores(self, job: Job) -> int:
        return int(math.ceil(job.num_of_processes / (self.cores_per_node / 2)) * (self.cores_per_node / 2))

    def full_node_cores(self, job: Job) -> int:
        return int(math.ceil(job.num_of_processes / self.cores_per_node) * self.cores_per_node)


    def deploy_to_waiting_queue(self, job_set: list[Job]) -> None:
        """Used to initialize the waiting queue of a cluster
        or to append more jobs to the queue. It also overrides
        the job ids of the jobs
        """
        copy = deepcopy_list(job_set)

        if self.makespan == 0:
            self.id_counter = 0

        for job in copy:
            job.job_id = self.id_counter
            job.queued_time = self.makespan
            self.id_counter += 1

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
        
        # Check if there are any jobs left waiting
        if self.waiting_queue != []:

            # If scheduler deployed jobs to execution list
            # then go to the next simulation loop
            if self.scheduler.deploy():
                return

            self.logger.evt_jobs_executing()

            # If the scheduler didn't deploy jobs then
            # the execution list is full and we have to
            # execute some
            self.next_state()
            # Free the resources
            self.free_resources()

        # If there aren't any jobs left on the waiting queue
        else:
            # Check if there is any job left in the execution queue
            if self.execution_list != []:

                self.logger.evt_jobs_executing()

                self.next_state()
                self.free_resources()

    def run(self):
        """The simulation loop
        """

        # Setup cluster
        self.setup()

        # Setup scheduling algorithms
        self.scheduler.setup()

        # Reset counters for an experiment
        self.logger.setup()

        # Simulation loop
        while self.waiting_queue != [] or self.execution_list != []:
            self.step()

