import os
import sys
sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), '../../')
))


from numpy.random import seed, randint
from time import time_ns

from realsim.jobs import Job
from realsim.scheduler.balancerFullOn import BalancerFullOn


class RandomScheduler(BalancerFullOn):

    name = "Random Co-Scheduler"
    description = "Random pairs co-scheduler"

    def wq_sort(self, job: Job):
        seed(time_ns() % (2 ** 32))
        return randint(len(self.cluster.waiting_queue))

    def coloc_ordering(self, job: Job, cojob: Job, ll_avg_speedup) -> float:
        seed(time_ns() % (2 ** 32))
        return float(randint(len(self.cluster.waiting_queue)))
