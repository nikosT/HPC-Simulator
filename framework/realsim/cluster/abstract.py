import abc
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from realsim.jobs import Job
from realsim.jobs.utils import deepcopy_list
from realsim.scheduler.scheduler import Scheduler
from realsim.logger.logger import Logger

from typing import List
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
        self.waiting_queue: List[Job] = list()
        # The queue of jobs that are executing
        self.execution_list: List[List[Job]] = list()
        # Finished jobs' ids list
        self.finished_jobs: List[int] = list()

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

    def deploy_to_waiting_queue(self, job_set: List[Job]) -> None:
        """Used to initialize the waiting queue of a cluster
        or to append more jobs to the queue. It also overrides
        the job ids of the jobs
        """
        copy = deepcopy_list(job_set)
        # Check if waiting_queue is empty
        if self.waiting_queue == []:
            # If it initializes then restart counter
            self.id_counter = 0
            self.waiting_queue = copy
            for job in self.waiting_queue:
                job.job_id = self.id_counter
                self.id_counter += 1
        else:
            for job in copy:
                job.job_id = self.id_counter
                self.id_counter += 1
                self.waiting_queue.append(job)

    def half_node_cores(self, job: Job) -> int:
        return int(math.ceil(job.num_of_processes / (self.cores_per_node / 2)) * (self.cores_per_node / 2))

    def full_node_cores(self, job: Job) -> int:
        return int(math.ceil(job.num_of_processes / self.cores_per_node) * self.cores_per_node)

    @abc.abstractmethod
    def next_state(self) -> None:
        pass

    @abc.abstractmethod
    def free_resources(self) -> None:
        pass

    def run_step(self):
        """Used mainly for debugging purposes
        """
        # Simulation loop
        if self.waiting_queue != [] or self.execution_list != []:

            # This is definite
            # If broken then the simulation loop has problems
            if self.free_cores < 0 or self.free_cores > self.total_cores:
                raise RuntimeError(f"Free cores: {self.free_cores}")
            
            # Check if there are any jobs left waiting
            if self.waiting_queue != []:

                # If scheduler deployed jobs to execution list
                # then go to the next simulation loop
                if self.scheduler.deploying():
                    return

                self.logger.jobs_start()

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

                    self.logger.jobs_start()

                    self.next_state()
                    self.free_resources()

    def run(self):
        """The simulation loop
        """

        self.free_cores = self.total_cores
        self.makespan = 0
        self.execution_list = list()

        # Reset counters for an experiment
        self.logger.init_logger()

        # Simulation loop
        while self.waiting_queue != [] or self.execution_list != []:

            # This is definite
            # If broken then the simulation loop has problems
            if self.free_cores < 0 or self.free_cores > self.total_cores:
                raise RuntimeError(f"Free cores: {self.free_cores}")
            
            # Check if there are any jobs left waiting
            if self.waiting_queue != []:

                # If scheduler deployed jobs to execution list
                # then go to the next simulation loop
                if self.scheduler.deploying():
                    continue

                self.logger.jobs_start()

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

                    self.logger.jobs_start()

                    self.next_state()
                    self.free_resources()

