from abc import ABC
from .ranks import RanksCoscheduler
from numpy.random import seed, randint
from time import time_ns
from math import inf
import os
import sys
from realsim.jobs.utils import deepcopy_list
from itertools import combinations

sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../../../../"
)))

from realsim.jobs.jobs import Job
from realsim.scheduler.coschedulers.ranks.ranks import RanksCoscheduler


class UtilCoscheduler(RanksCoscheduler, ABC):

    name = "Util Co-Scheduler"
    description = """Co-scheduling favoring Utilization filling"""

    def waiting_queue_reorder(self, job: Job) -> float:
        # seed(time_ns() % (2 ** 32))
        # return float(randint(len(self.cluster.waiting_queue)))
	    return 1.0

    def host_alloc_condition(self, hostname: str, job: Job) -> (float,float):
        """Condition on how to sort the hosts based on the speedup that the job
        will gain/lose. Always spread first
        """

        # get the jobs signatures that are assinged to the host
        co_job_sigs = list(self.cluster.hosts[hostname].jobs.keys())

        # If no signatures then spread
        if co_job_sigs == []:
            return (job.max_speedup, inf)

        # get average speedup for each job in host + candidate job
        speedup = list(map(lambda j: self.database.heatmap[job.job_name][j.split(":")[-1]],co_job_sigs))
        speedup += list(map(lambda j: self.database.heatmap[j.split(":")[-1]][job.job_name],co_job_sigs))
        avg_speedup = sum(speedup) / (len(co_job_sigs)*2)

        # get how many of the speedup values of pairs job,x and x,job for each x in host are bellow threshold
        jobs_with_speedup = list(filter(lambda j: self.database.heatmap[job.job_name][j.split(":")[-1]] >= 1,co_job_sigs))
        jobs_with_speedup += list(filter(lambda j: self.database.heatmap[j.split(":")[-1]][job.job_name] >= 1,co_job_sigs))
        speedup_counts = len(jobs_with_speedup)

        return (avg_speedup, speedup_counts)

    def deploy(self) -> bool:
        # it uses the "waiting_queue_reorder"
        # it uses the "allocation" which uses the "host_alloc_condition"
        def sf(values, k=1):
            return round(len(list(filter(lambda x: x > k, values)))/len(values), 2)
            
        def get_job(id: int, jobs: list) -> Job:
            return next((j for j in jobs if j.job_id == id), None)
        
        def speedup_score(j1: Job, j2: Job, compact=False) -> float:
            area1 = j1.num_of_processes * j1.remaining_time
            area2 = j2.num_of_processes * j2.remaining_time
            if compact:
                return 1
            return (area1 * self.database.heatmap[j1.job_name][j2.job_name] +\
                    area2 * self.database.heatmap[j2.job_name][j1.job_name]) / (area1 + area2)
            
        self.queue_depth = 10

        waiting_queue = deepcopy_list(self.cluster.waiting_queue[:self.queue_depth])
        execution_list = deepcopy_list(self.cluster.execution_list)
        heatmap_list = waiting_queue + execution_list

        waiting_ids = list(map(lambda j: j.job_id, waiting_queue))
        execution_ids = list(map(lambda j: j.job_id, execution_list))
        heatmap_ids = waiting_ids + execution_ids

        h_combs = set(combinations(heatmap_ids, 2)) - set(combinations(execution_ids, 2))

        # calculate heatmap score
        scores = list(map(lambda c: speedup_score(get_job(c[0], heatmap_list), get_job(c[1], heatmap_list)), h_combs))
        print(sf(scores, 1))

        # 0. if old, you have to deploy it
        # 1. compact vs co-sched
        # a. get wait and exec queue
        # b. create all possible subheatmaps (depth=3) wait+exec * wait+exec
        # c. calc sf keep the best
        # d. job can now be scheduled as compact or co-sched

        # 2. how to co-sched
        #   a. best fit (utilization)
        



        return super().deploy()

    def backfill(self) -> bool:
        return super().backfill()

    def after_deployment(self, *args):
        return super().after_deployment(*args)
