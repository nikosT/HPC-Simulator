from .__init__ import *

from .abstract import AbstractGenerator
from random import shuffle, seed
from time import time_ns

class KeysListGenerator(AbstractGenerator[str]):

    name = "List Generator"
    description = "Generate jobs based on the list of names given by the user"

    def __init__(self, load_manager):
        AbstractGenerator.__init__(self, load_manager=load_manager)

    def generate_jobs_set(self, arg: list[tuple[str, float]]) -> list[Job]:
        """Generate jobs based on the names in the dictionary and their
        frequency of appeareance.

        - arg: a list of names of loads and their submission time
        """

        # Create a list of jobs based on arg
        jobs_set = list()
        idx = 0
        for load_name, submit_time in arg:
            jobs_set.append(
                    self.generate_job(idx, self.load_manager(load_name), submit_time)
            )

        return jobs_set

