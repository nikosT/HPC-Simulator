import os
import sys

sys.path.append(os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "../../../"
        )
    ))

from api.loader import Load
from typing import Optional
from numpy import average as avg
from numpy import isnan


class Job:

    def __init__(self, load: Optional[Load], job_id, job_name, num_of_processes,
                 remaining_time, queued_time, waiting_time, wall_time, binded_cores):
        self.load = load
        self.job_id = job_id
        self.job_name = job_name
        self.num_of_processes = num_of_processes
        self.remaining_time = remaining_time
        self.queued_time = queued_time
        self.waiting_time = waiting_time
        self.wall_time = wall_time
        self.binded_cores = binded_cores
        self.gave_position = 0
        self.speedup = 1

    def __eq__(self, job):
        if not isinstance(job, Job):
            return False
        return self.load == job.load and self.job_id == job.job_id and self.job_name == job.job_name and self.num_of_processes == job.num_of_processes\
                and self.remaining_time == job.remaining_time and self.queued_time == job.queued_time\
                and self.wall_time == job.wall_time and self.speedup == job.speedup

    def __repr__(self) -> str:
        return "{" + f"{self.job_id}, {self.job_name} : {self.remaining_time}, {self.speedup}, {self.binded_cores}" + "}"


    def get_speedup(self, cojob):
        return self.load.get_median_speedup(cojob.job_name)
        # return avg(self.load.get_speedups(cojob.job_name))

    def get_overall_speedup(self):
        speedups = list()
        for coload in self.load.coloads:
            speedups.append(self.load.get_median_speedup(coload))
        return avg(speedups)

    def get_max_speedup(self):
        speedups = list()
        for coload in self.load.coloads:
            speedups.append(
                    self.load.get_median_speedup(coload)
            )

        return max(speedups)

    def ratioed_remaining_time(self, cojob):
        old_speedup = self.speedup
        new_speedup = self.get_speedup(cojob)
        if old_speedup <= 0 or new_speedup <= 0 or isnan(old_speedup) or isnan(new_speedup):
            raise RuntimeError(f"{old_speedup}, {new_speedup}")
        self.remaining_time *= (old_speedup / new_speedup)
        self.speedup = new_speedup

    def deepcopy(self):
        """Return a new instance of Job that is a true copy
        of the original
        """
        copy = Job(load=self.load.deepcopy(),
                   job_id=self.job_id,
                   job_name=self.job_name,
                   num_of_processes=self.num_of_processes,
                   remaining_time=self.remaining_time,
                   queued_time=self.queued_time,
                   waiting_time=self.waiting_time,
                   wall_time=self.wall_time,
                   binded_cores=self.binded_cores)

        copy.gave_position = self.gave_position
        copy.speedup = self.speedup

        return copy


class EmptyJob(Job):

    def __init__(self, job: Job):
        Job.__init__(self, None, job.job_id, job.job_name, job.num_of_processes,
                     None, None, None, None, job.binded_cores)

    def __repr__(self) -> str:
        return "{" + f"{self.job_id}, empty : {self.remaining_time}, {self.binded_cores}" + "}"

    def deepcopy(self):
        """Return a new instance of Job that is a true copy
        of the original
        """
        copy = EmptyJob(Job(load=None,
                            job_id=self.job_id,
                            job_name=self.job_name,
                            num_of_processes=self.num_of_processes,
                            remaining_time=self.remaining_time,
                            queued_time=self.queued_time,
                            waiting_time=self.waiting_time,
                            wall_time=self.wall_time,
                            binded_cores=self.binded_cores)
                        )

        return copy
