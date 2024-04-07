from numpy.random import seed, randint
from time import time_ns
import os
import sys

sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../../../../"
)))

from realsim.jobs.jobs import Job
from realsim.scheduler.coschedulers.ranks.ranks import RanksCoscheduler


class RandomRanksCoscheduler(RanksCoscheduler):

    name = "Random Ranks Co-Scheduler"
    descriptions = ""

    def xunits_order(self, xunit: list[Job]) -> float:
        seed(time_ns() % (2 ** 32))
        return float(randint(len(self.cluster.waiting_queue)))

    def xunits_candidates_order(self, largest_job: Job, job: Job) -> float:
        seed(time_ns() % (2 ** 32))
        return float(randint(len(self.cluster.waiting_queue)))

    def waiting_queue_order(self, job: Job) -> float:
        seed(time_ns() % (2 ** 32))
        return float(randint(len(self.cluster.waiting_queue)))

    def wjob_candidates_order(self, job: Job, co_job: Job) -> float:
        seed(time_ns() % (2 ** 32))
        return float(randint(len(self.cluster.waiting_queue)))
