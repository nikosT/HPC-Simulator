# Utilities
from datetime import timedelta
from math import inf, ceil
import numpy as np

# Simulation
from procset import ProcSet
from realsim.jobs.jobs import Job, JobCharacterization, JobState
from realsim.jobs.utils import deepcopy_list
from realsim.cluster.host import Host
from realsim.cluster.cluster import Cluster
from realsim.database import Database
from realsim.scheduler.scheduler import Scheduler
from realsim.logger.logger import Logger, LogEvent


class ComputeEngine:

    def __new__(cls, 
                db: Database,
                cluster: Cluster,
                scheduler: Scheduler,
                logger: Logger):

        cls.db = db
        cls.cluster = cluster
        cls.scheduler = scheduler
        cls.logger = logger
        cls.makespan: float = 0

        return object.__new__(cls)


    @classmethod
    def __log_details(cls, msg: str) -> str:
        return f"({timedelta(seconds=cls.makespan)})    {msg}"

    # Database preloaded queue setup
    def setup_preloaded_jobs(self) -> None:
        """Setup the preloaded jobs that are currently stored in the database
        """

        # Sort jobs by their time they will be appearing in the waiting queue
        self.db.preloaded_queue.sort(key=lambda job: job.submit_time)

        if self.makespan == 0:
            self.id_counter = 0

        # Preload jobs and calculate their respective half and full node cores
        # usage
        for job in self.db.preloaded_queue:

            # Set job id
            job.job_id = self.id_counter

            # Setup core resources needed
            job.full_socket_nodes = ceil(job.num_of_processes / sum(self.cluster.full_socket_allocation))
            job.half_socket_nodes = ceil(job.num_of_processes / sum(self.cluster.half_socket_allocation))

            # Setup job speedups
            speedups = list(self.db.heatmap[job.job_name].values())
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

            # Setup job characterization
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

        copy = deepcopy_list(self.db.preloaded_queue)

        for job in copy:
            if job.submit_time <= self.makespan:
                # Infinite waiting queue size
                if self.cluster.queue_size == -1:
                    self.db.preloaded_queue.remove(job)
                    self.cluster.waiting_queue.append(job)

                # Zero size waiting queue
                elif self.cluster.queue_size == 0 and\
                        len(self.cluster.waiting_queue) == 0 and\
                        self.scheduler.find_suitable_nodes(job.num_of_processes, self.cluster.full_socket_allocation) != {}:
                            self.db.preloaded_queue.remove(job)
                            job.submit_time = self.makespan
                            self.cluster.waiting_queue.append(job)

                # Finite waiting queue size but not 0
                elif self.cluster.queue_size > 0 and len(self.cluster.waiting_queue) < self.cluster.queue_size:
                        self.db.preloaded_queue.remove(job)
                        job.submit_time = self.makespan
                        self.cluster.waiting_queue.append(job)

                else:
                    break


    # Job execution/deploying/cleaning computations
    @classmethod
    def calculate_job_rem_time(cls, job: Job) -> None:

        # The worst possible speedup
        worst_speedup = job.max_speedup

        # Important flags
        neighbors_exist = False
        spread_allocation = not (job.socket_conf == cls.cluster.socket_conf)

        for hostname in job.assigned_hosts:
            for co_job_signature in cls.cluster.hosts[hostname].jobs.keys():

                # Shouldn't check with ourselves
                if job.job_signature == co_job_signature:
                    continue

                neighbors_exist = True

                co_job_name = co_job_signature.split(":")[-1]
                speedup = cls.db.heatmap[job.job_name][co_job_name]
                # If we do not have knowledge of the job's speedup when co-allocated
                # to the specific co-job then use the average speedup
                if speedup is None:
                    speedup = job.avg_speedup
                if speedup < worst_speedup:
                    worst_speedup = speedup

        # Recalculate the remaining time of the job and the current speedup

        # If there exist neighbors beside the job in one of the hosts then
        # co-execution
        if neighbors_exist:
            # Change only if the worst speedup is different from the current
            # speedup of the job
            if job.sim_speedup != worst_speedup:
                job.remaining_time *= (job.sim_speedup / worst_speedup)
                job.sim_speedup = worst_speedup
        # If no neighbors exist
        else:
            # If it is spread allocated check to change the rem time
            if spread_allocation:
                # Change if it had neighbors but now it is executing alone
                if job.sim_speedup != worst_speedup:
                    job.remaining_time *= (job.sim_speedup / worst_speedup)
                    job.sim_speedup = worst_speedup

    @classmethod
    def deploy_job_to_host(cls, hostname: str, job: Job, psets: list[ProcSet]) -> None:

        # Log the event
        cls.logger.log(LogEvent.JOB_LOG, cls.__log_details("Job[{job.job_signature}] started execution"))
        cls.logger.log(LogEvent.CLUSTER_LOG, cls.__log_details("Job[{job.job_signature}] started execution in host[{hostname}]"))

        # Set the start time of execution of the job
        job.start_time = cls.makespan

        # Add job signature to the host and the processor set it allocates
        cls.cluster.hosts[hostname].jobs.update({
            job.job_signature: psets
        })

        # Remove psets from host
        for i, socket_pset in enumerate(cls.cluster.hosts[hostname].sockets):
            socket_pset -= psets[i]

        # Set state of host
        cls.cluster.hosts[hostname].state = Host.ALLOCATED

        # Add the job to the execution list of the cluster
        if job.current_state == JobState.PENDING:
            cls.cluster.waiting_queue.remove(job)
            cls.cluster.execution_list.append(job)
            job.current_state = JobState.EXECUTING

    @classmethod
    def clean_job_from_hosts(cls, job: Job) -> None:

        # Log the event
        cls.logger.log(LogEvent.JOB_LOG, cls.__log_details("Job[{job.job_signature}] finished execution"))

        # Set the finish time of the job
        job.finish_time = cls.makespan
        job.current_state = JobState.FINISHED

        # Clean job and return resources back to host
        for hostname in job.assigned_hosts:
            # Log the event
            cls.logger.log(LogEvent.CLUSTER_LOG, cls.__log_details("Job[{job.job_signature}] finished execution in host[{hostname}]"))

            # Return the allocated processors of a job to each host
            for i, pset in enumerate(cls.cluster.hosts[hostname].jobs[job.job_signature]):
                cls.cluster.hosts[hostname].sockets[i].union(pset)

            # Remove job signature from host
            cls.cluster.hosts[hostname].jobs.pop(job.job_signature)
            
            # Change state of host if nothing is executing
            if len(cls.cluster.hosts[hostname].jobs.keys()) == 0:
                cls.cluster.hosts[hostname].state = Host.IDLE


        # Remove job from the execution list of the cluster
        cls.cluster.execution_list.remove(job)


    # Simulation loop computations
    def goto_next_sim_state(self) -> None:

        # Recalculate the remaining time of jobs
        for job in self.cluster.execution_list:
            self.calculate_job_rem_time(job)

        # Find the minimum remaining execution time of the jobs currently executing
        min_rem_time = inf
        for job in self.cluster.execution_list:
            if job.remaining_time < min_rem_time:
                min_rem_time = job.remaining_time

        # Find the minimum remaining time for a job to show up in the waiting
        # queue of the cluster
        for job in self.db.preloaded_queue:
            showup_time = job.submit_time - self.makespan
            if showup_time > 0 and showup_time < min_rem_time:
                min_rem_time = showup_time
        
        # Guard the execution
        assert min_rem_time > 0 and min_rem_time < inf

        # Schedulers' aging mechinsims
        # TODO: CHECK THE CORRECTNESS OF CODE!!
        # It was copied by the previous implementation but the complexity of the
        # code may leave open difficult to understand issues
        #####
        # If there is an aging mechanism in the scheduling algorithm
        if self.scheduler.aging_enabled and len(self.cluster.waiting_queue) > 0 and self.cluster.waiting_queue[0].age < self.scheduler.age_threshold:
            # Find the interval until the next scheduler step
            scheduler_timer = int(self.makespan / self.scheduler.time_step) * self.scheduler.time_step
            next_shd_step = (scheduler_timer + self.scheduler.time_step) - self.makespan
            # Find how much time should pass for the head job to reach the
            # maximum age for compact allocation
            max_age_step = next_shd_step + (self.scheduler.age_threshold - (self.cluster.waiting_queue[0].age + 1)) * self.scheduler.time_step
            # If the time it takes to reach is less than the min_rem_time then
            # re-enact deployment
            if max_age_step < min_rem_time:
                min_rem_time = max_age_step
                self.cluster.waiting_queue[0].age = self.scheduler.age_threshold
                # print(self.cluster.waiting_queue[0].job_id, self.cluster.waiting_queue[0].job_name, self.makespan)

        # Forward the time of the execution
        self.makespan += min_rem_time

        # Log the event
        self.logger.log(LogEvent.COMPENG_LOG, self.__log_details(f"Caclulated the next step time to {min_rem_time}"))


        # "Execute" the jobs
        execution_list: list[Job] = list()

        # Remove/clean any jobs that finished execution
        for job in self.cluster.execution_list:

            # "Execute" job
            job.remaining_time -= min_rem_time

            if job.remaining_time == 0:
                self.clean_job_from_hosts(job)
            else:
                execution_list.append(job)

        # Recalculate the remaining time of jobs if they are co-scheduled or spread
        for job in execution_list:
            self.calculate_job_rem_time(job)

        # Assign new execution list to cluster
        self.cluster.execution_list = execution_list

    def sim_step(self) -> None:

        deployed = False

        # Deploy to waiting queue any preloaded jobs that remain
        self.load_in_waiting_queue()
        
        # Check if there are any jobs left waiting
        if self.cluster.waiting_queue != []:

            # Deploy/Submit jobs to the execution list
            deployed = self.scheduler.deploy()

            # If the backfilling policy of a scheduler is enabled
            if self.scheduler.backfill_enabled:

                # Execute the backfilling algorithm
                deployed |= self.scheduler.backfill()

        # If deployed restart scheduling procedure
        if deployed:
            return

        # If the scheduler didn't deploy jobs then
        # 1. the cluster's execution list is full
        # 2. There are no jobs in the waiting queue but there are in the 
        #    queue preloaded
        self.goto_next_sim_state()
