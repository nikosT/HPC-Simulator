import os
import sys

sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../../../"
)))

from realsim.jobs.utils import deepcopy_list
from .fifo import FIFOScheduler
from math import inf


class EASYScheduler(FIFOScheduler):

    name = "EASY Scheduler"
    description = "FIFO Scheduler with EASY backfilling policy"

    def __init__(self):
        FIFOScheduler.__init__(self)
        self.backfill_enabled = True


    def backfill(self) -> None:

        if len(self.cluster.waiting_queue) <= 1:
            return

        execution_list = deepcopy_list(self.cluster.execution_list)
        execution_list.sort(key=lambda jblock: jblock[0].wall_time +
                            jblock[0].start_time - self.cluster.makespan)

        blocked_job = self.cluster.waiting_queue[0]

        # Find the minimum estimated start time of the job
        aggr_cores = len(self.cluster.total_procs)
        min_estimated_time = inf

        for xunit in execution_list:

            running_job = xunit[0]
            aggr_cores += len(running_job.assigned_cores)

            if aggr_cores >= blocked_job.full_node_cores:
                min_estimated_time = running_job.wall_time - (self.cluster.makespan - running_job.start_time)
                break

        # If a job couldn't reserve cores then cancel backfill at this point
        if not min_estimated_time < inf:
            return

        # Find job(s) that can backfill the execution list

        # Get the backfilling candidates
        backfilling_jobs = deepcopy_list(self.cluster.waiting_queue[1:])

        # Ascending sorting by their wall time
        backfilling_jobs.sort(key=lambda b_job: b_job.wall_time)

        for b_job in backfilling_jobs:

            if b_job.wall_time <= min_estimated_time:

                procset = self.assign_nodes(b_job.full_node_cores, self.cluster.total_procs)

                if procset is not None:

                    self.cluster.waiting_queue.remove(b_job)
                    b_job.start_time = self.cluster.makespan
                    b_job.assigned_cores = procset
                    self.cluster.execution_list.append([b_job])
                    self.cluster.total_procs -= procset

            else:
                # No other job is capable to backfill based on time
                break
        
        return

