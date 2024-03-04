from .__init__ import *

from .abstract import AbstractGenerator
from random import shuffle, seed
from time import time_ns

class KeysListGenerator(AbstractGenerator[list]):

    name = "List Generator"
    description = "Generate jobs based on the list of names given by the user"

    def __init__(self, load_manager):
        AbstractGenerator.__init__(self, load_manager=load_manager)

    def generate_jobs_set(self, arg: list[str]) -> list[Job]:
        """Generate jobs based on the names in the dictionary and their
        frequency of appeareance.

        - arg: a dictionary with names of loads and their frequency of
          appeareance
        """

        # Create a list of jobs based on arg
        jobs_set = list()
        for idx, load_name in enumerate(arg):
            jobs_set.append(
                    self.generate_job(idx, self.load_manager(load_name))
            )

        return jobs_set

