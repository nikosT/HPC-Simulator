# Global libraries
import abc
from numpy.random import seed, randint, random_sample
from time import time_ns
from typing import TypeVar, Generic
from .__init__ import *

T = TypeVar("T")


class AbstractGenerator(abc.ABC, Generic[T]):

    name = "Abstract Generator"
    description = "Abstract base class for all generators"

    def __init__(self, load_manager: LoadManager):
        self.load_manager = load_manager

    def generate_job(self, idx: int, load: Load):
        seed(time_ns() % (2**32))
        return Job(load=load,
                   job_id=idx,
                   job_name=load.full_load_name,
                   num_of_processes=load.num_of_processes,
                   remaining_time=load.get_avg(),
                   queued_time=(randint(100) * random_sample()),
                   wall_time=(10 * 60),
                   binded_cores=load.num_of_processes)

    @abc.abstractmethod
    def generate_jobs_set(self, arg: T) -> list[Job]:
        """Generate a set of num_of_jobs jobs based on the workloads stored in load_manager
        """
        pass
