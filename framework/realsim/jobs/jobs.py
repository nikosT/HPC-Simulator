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
from procset import ProcSet


class JobTag:
    COMPACT = 0
    SPREAD = 1
    ROBUST = 2
    FRAIL = 3


class Job:

    def __init__(self, 
                 load: Optional[Load], 
                 job_id: int, 
                 job_name: str, 
                 num_of_processes: int,
                 binded_cores: int,
                 assigned_cores: ProcSet,
                 full_node_cores: int,
                 half_node_cores: int,
                 remaining_time, 
                 submit_time, 
                 waiting_time, 
                 wall_time):

        self.load = load

        self.job_id = job_id
        self.job_name = job_name

        self.num_of_processes = num_of_processes
        self.binded_cores = binded_cores
        self.full_node_cores = full_node_cores
        self.half_node_cores = half_node_cores
        self.assigned_cores = assigned_cores

        self.remaining_time = remaining_time
        self.submit_time = submit_time
        self.waiting_time = waiting_time
        self.wall_time = wall_time

        self.start_time: float = 0.0
        self.speedup = 1

        self.job_tag = JobTag.COMPACT


    def __eq__(self, job):
        if not isinstance(job, Job):
            return False
        return self.load == job.load and self.job_id == job.job_id and self.job_name == job.job_name and self.num_of_processes == job.num_of_processes\
                and self.binded_cores == job.binded_cores\
                and self.remaining_time == job.remaining_time and self.submit_time == job.submit_time\
                and self.wall_time == job.wall_time and self.start_time == job.start_time and self.speedup == job.speedup\
                and self.job_tag == job.job_tag

    def __repr__(self) -> str:
        return "{" + f"{self.job_id}, {self.job_name} : {self.remaining_time}, {self.speedup}, {self.binded_cores}" + "}"


    def get_speedup(self, cojob):
        return self.load.get_med_speedup(cojob.job_name)

    def get_overall_speedup(self) -> float:
        speedups: list[float] = list()
        for coload in self.load.coscheduled_timelogs:
            speedups.append(self.load.get_med_speedup(coload))
        return float(avg(speedups))

    def get_max_speedup(self) -> float:
        speedups: list[float] = list()
        for coload in self.load.coscheduled_timelogs:
            speedups.append(
                    self.load.get_med_speedup(coload)
            )

        return max(speedups)

    def get_min_speedup(self):
        speedups: list[float] = list()
        for coload in self.load.coscheduled_timelogs:
            speedups.append(
                    self.load.get_med_speedup(coload)
            )

        return min(speedups)

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
        copy = Job(load=self.load,
                   job_id=self.job_id,
                   job_name=self.job_name,
                   num_of_processes=self.num_of_processes,
                   binded_cores=self.binded_cores,
                   assigned_cores=self.assigned_cores,
                   full_node_cores=self.full_node_cores,
                   half_node_cores=self.half_node_cores,
                   remaining_time=self.remaining_time,
                   submit_time=self.submit_time,
                   waiting_time=self.waiting_time,
                   wall_time=self.wall_time)

        copy.start_time = self.start_time
        copy.speedup = self.speedup
        copy.job_tag = self.job_tag

        return copy


class EmptyJob(Job):

    def __init__(self, job: Job):
        Job.__init__(self, 
                     None, 
                     job.job_id, 
                     job.job_name, 
                     job.num_of_processes, 
                     job.binded_cores, 
                     job.assigned_cores,
                     -1, 
                     -1, 
                     None, 
                     None, 
                     None, 
                     None)

    def __repr__(self) -> str:
        return "{" + f"{self.job_id}, idle : {self.remaining_time}, {self.binded_cores}" + "}"

    def deepcopy(self):
        """Return a new instance of Job that is a true copy
        of the original
        """
        copy = EmptyJob(Job(load=None,
                            job_id=self.job_id,
                            job_name=self.job_name,
                            num_of_processes=self.num_of_processes,
                            binded_cores=self.binded_cores,
                            assigned_cores=self.assigned_cores,
                            full_node_cores=self.full_node_cores,
                            half_node_cores=self.half_node_cores,
                            remaining_time=self.remaining_time,
                            submit_time=self.submit_time,
                            waiting_time=self.waiting_time,
                            wall_time=self.wall_time)
                        )

        return copy
