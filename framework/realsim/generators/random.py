from .__init__ import *

from .abstract import AbstractGenerator
from numpy.random import seed, randint, random_sample
from time import time_ns


class RandomGenerator(AbstractGenerator[int]):

    name = "Random Generator"
    description = "Generating random set of jobs from a specific LoadManager instance"

    def __init__(self, load_manager: LoadManager):
        AbstractGenerator.__init__(self, load_manager=load_manager)

    def generate_jobs_set(self, arg: int) -> list[Job]:
        # Get the load names of the load_manager
        keys = list(self.load_manager.loads.keys())

        # Generate random positive integers that will be used as
        # indices to query the loads' names
        seed(time_ns() % (2 ** 32))
        ints = randint(low=0, high=len(keys), size=(arg,))

        # Get the names of the loads
        names = list(map(lambda i: keys[i], ints))
        # Get the loads
        loads = list(map(lambda name: self.load_manager(name), names))

        jobs_set: list[Job] = list()
        for i, load in enumerate(loads):
            jobs_set.append(
                    self.generate_job(i, load)
            )

        # Sort the jobs by their queued time
        jobs_set.sort(key=(lambda job: job.queued_time))
        for jid, job in enumerate(jobs_set):
            job.job_id = jid

        return jobs_set
