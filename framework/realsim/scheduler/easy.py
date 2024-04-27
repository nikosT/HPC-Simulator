from realsim.jobs.utils import deepcopy_list
from .fifo import FIFOScheduler
from math import inf


class EASYScheduler(FIFOScheduler):

    name = "EASY Scheduler"
    description = "FIFO Scheduler with EASY backfilling policy"

    def __init__(self):
        FIFOScheduler.__init__(self)
        self.backfill_enabled = True


    def backfill(self):

        waiting_queue = deepcopy_list(self.cluster.waiting_queue)
        total_reserved_cores = 0

        while waiting_queue != [] and self.cluster.free_cores > 0:

            job = self.pop(waiting_queue)

            # Find the minimum estimated start time of the job
            min_estimated_time = inf

            for jobs_block in self.cluster.execution_list:

                running_job = jobs_block[0]

                if job.full_node_cores <= running_job.binded_cores and (running_job.wall_time - running_job.start_time) < min_estimated_time:
                    min_estimated_time = running_job.wall_time - running_job.start_time

            # If a job couldn't reserve cores then cancel backfill at this point
            if min_estimated_time < inf:
                break

            # Find a job that can backfill the execution list

            # Get the backfilling candidates
            backfilling_jobs = deepcopy_list(waiting_queue)

            # Ascending sorting by their wall time
            backfilling_jobs.sort(key=lambda b_job: b_job.wall_time)

            for b_job in backfilling_jobs:

                if b_job.wall_time <= min_estimated_time:

                    if b_job.full_node_cores <= self.cluster.free_cores:

                        waiting_queue.remove(b_job)
                        self.cluster.waiting_queue.remove(b_job)
                        b_job.start_time = self.cluster.makespan
                        b_job.binded_cores = b_job.full_node_cores
                        self.cluster.execution_list.append([b_job])
                        self.cluster.free_cores -= b_job.binded_cores
                else:
                    # No other job is capable to backfill based on time
                    break

