from abc import ABC
from .ranks import RanksCoscheduler
from numpy.random import seed, randint
from time import time_ns
import os
import sys

sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../../../../"
)))

from realsim.jobs.jobs import Job
from realsim.scheduler.coschedulers.ranks.ranks import RanksCoscheduler


class RandomRanksCoscheduler(RanksCoscheduler, ABC):

    name = "Random Ranks Co-Scheduler"
    description = """Random co-scheduling using ranks architecture as a fallback
    to classic scheduling algorithms"""

    def waiting_queue_reorder(self, job: Job) -> float:
        # seed(time_ns() % (2 ** 32))
        # return float(randint(len(self.cluster.waiting_queue)))
	    return 1.0

   # def coloc_condition(self, hostname: str, job: Job) -> float:
   #     return 1.0
