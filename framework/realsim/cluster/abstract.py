import abc
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

from realsim.jobs import Job, EmptyJob, JobCharacterization
from realsim.jobs.utils import deepcopy_list
from realsim.scheduler.scheduler import Scheduler
from realsim.logger.logger import Logger
from realsim.database import Database

import math
import numpy as np
from procset import ProcSet


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

        self.total_procs: ProcSet = ProcSet((1, self.total_cores))

        # Database instance of cluster
        self.database: Database

        # Scheduler instance for the cluster
        self.scheduler: Scheduler

        # Logger instance for the cluster
        self.logger: Logger

        # Waiting queue size
        self.queue_size = math.inf
        # The generated jobs are first deployed here
        self.waiting_queue: list[Job] = list()
        # The queue of jobs that are executing
        self.execution_list: list[list[Job]] = list()
        # Finished jobs' ids list
        self.finished_jobs: int = 0

        # Important counters #

        # Job id counter
        self.id_counter: int = 0
        
        # The total execution time
        # of a cluster
        self.makespan: float = 0

    def assign_database(self, db: Database) -> None:
        self.database = db

    def assign_scheduler(self, scheduler: Scheduler):
        self.scheduler = scheduler
        self.scheduler.assign_cluster(self)

    def assign_logger(self, logger: Logger):
        self.logger = logger
        self.logger.assign_cluster(self)

    def setup_preloaded_jobs(self) -> None:
        """Setup the preloaded jobs that are currently stored in the database
        """

        # Sort jobs by their time they will be appearing in the waiting queue
        self.database.preloaded_queue.sort(key=lambda job: job.submit_time)

        if self.makespan == 0:
            self.id_counter = 0

        # Preload jobs and calculate their respective half and full node cores
        # usage
        for job in self.database.preloaded_queue:
            job.job_id = self.id_counter
            job.half_node_cores = int(math.ceil(job.num_of_processes / (self.cores_per_node / 2)) * (self.cores_per_node / 2))
            job.full_node_cores = int(math.ceil(job.num_of_processes / self.cores_per_node) * self.cores_per_node)

            if job.num_of_processes <= 512:
                job.age = 1

            speedups = list(self.database.heatmap[job.job_name].values())
            max_speedup = min_speedup = speedups[0]
            accumulator = length = 0
            for speedup in speedups:
                if speedup > max_speedup:
                    max_speedup = speedup
                if speedup < min_speedup:
                    min_speedup = speedup

                accumulator += speedup
                length += 1

            job.max_speedup = max_speedup
            job.min_speedup = min_speedup
            job.avg_speedup = (accumulator / length)

            avg = job.avg_speedup
            std = round(float(np.std(speedups)), 2)

            if avg > 1.02:
                job.job_character = JobCharacterization.SPREAD
            elif avg < 0.98:
                job.job_character = JobCharacterization.COMPACT
            else:
                if std > 0.07:
                    job.job_character = JobCharacterization.FRAIL
                else:
                    job.job_character = JobCharacterization.ROBUST

            self.id_counter += 1

    def load_in_waiting_queue(self) -> None:

        copy = deepcopy_list(self.database.preloaded_queue)

        for job in copy:
            if job.submit_time <= self.makespan:
                # Infinite waiting queue size
                if self.queue_size == -1:
                    self.database.preloaded_queue.remove(job)
                    self.waiting_queue.append(job)

                # Zero size waiting queue
                elif self.queue_size == 0 and\
                        len(self.waiting_queue) == 0 and\
                        self.scheduler.assign_nodes(job.full_node_cores, self.total_procs) is not None:
                            self.database.preloaded_queue.remove(job)
                            job.submit_time = self.makespan
                            self.waiting_queue.append(job)

                # Finite waiting queue size but not 0
                elif self.queue_size > 0 and len(self.waiting_queue) < self.queue_size:
                        self.database.preloaded_queue.remove(job)
                        job.submit_time = self.makespan
                        self.waiting_queue.append(job)

                else:
                    break

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

    def ratio_rem_time(self, job: Job, co_job: Job|str) -> None:
        old_speedup = job.sim_speedup
        if type(co_job) == Job:
            new_speedup = self.database.heatmap[job.job_name][co_job.job_name]
        elif type(co_job) == str:
            if co_job == 'max':
                new_speedup = job.get_max_speedup()
            elif co_job == 'min':
                new_speedup = job.get_min_speedup()
            elif co_job == 'avg':
                new_speedup = job.get_avg_speedup()
            else:
                raise RuntimeError(f"{co_job} : Unknown ratio policy")

        else:
            raise RuntimeError(f"{type(co_job)} is not appropriate type. Job or string are acceptable types")

        # if old_speedup <= 0 or new_speedup <= 0 or isnan(old_speedup) or isnan(new_speedup):
        if old_speedup <= 0 or new_speedup <= 0:
            raise RuntimeError(f"{old_speedup}, {new_speedup}")

        job.remaining_time *= (old_speedup / new_speedup)
        job.sim_speedup = new_speedup


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

        deployed = False

        # This is definite
        # If broken then the simulation loop has problems
        if self.free_cores < 0 or self.free_cores > self.total_cores:
            raise RuntimeError(f"Free cores: {self.free_cores}")

        # Deploy to waiting queue any preloaded jobs that remain
        self.load_in_waiting_queue()
        
        # Check if there are any jobs left waiting
        if self.waiting_queue != []:

            # Deploy/Submit jobs to the execution list
            deployed = self.scheduler.deploy()

            # If scheduler deployed jobs to execution list successfully and the
            # backfilling policy is enabled
            if self.scheduler.backfill_enabled:

                # Execute the backfilling algorithm
                deployed |= self.scheduler.backfill()

        # If deployed restart scheduling procedure
        if deployed:
            return

        self.logger.evt_jobs_executing()

        # If the scheduler didn't deploy jobs then
        # the execution list is full and we have to
        # execute some
        self.next_state()

        if self.execution_list != []:
            # Free the resources
            self.free_resources()

        #print(self.total_procs)
