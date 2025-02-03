from abc import ABC
from .ranks import RanksCoscheduler
from numpy.random import seed, randint
from time import time_ns
from math import inf
import os
import sys
from realsim.jobs.utils import deepcopy_list
from itertools import combinations, chain
from numpy import mean
from math import ceil

sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../../../../"
)))

from realsim.jobs.jobs import Job
from realsim.scheduler.coschedulers.ranks.ranks import RanksCoscheduler


class UtilCoscheduler(RanksCoscheduler, ABC):

    name = "Util Co-Scheduler"
    description = """Co-scheduling favoring Utilization filling"""
    queue_depth = 100

    def waiting_queue_reorder(self, job: Job) -> float:          
        def speedup_score(j1: Job, j2: Job, compact=False) -> float:
            area1 = j1.num_of_processes * j1.remaining_time
            area2 = j2.num_of_processes * j2.remaining_time
            if compact:
                return 1
            return (area1 * self.database.heatmap[j1.job_name][j2.job_name] +\
                    area2 * self.database.heatmap[j2.job_name][j1.job_name]) / (area1 + area2)  

        if self.cluster.waiting_queue[:self.queue_depth]:
            return min(list(map(lambda j: speedup_score(job, j),self.cluster.waiting_queue[:self.queue_depth])))
        else:
            return inf

    def host_alloc_condition(self, hostname: str, job: Job) -> (float,float):
        """Condition on how to sort the hosts based on the speedup that the job
        will gain/lose. Always spread first
        """
        return super().host_alloc_condition(hostname, job)

    def deploy(self) -> bool:
        return super().deploy()

    def backfill(self) -> bool:
        return super().backfill()

    def after_deployment(self, *args):
        return super().after_deployment(*args)
