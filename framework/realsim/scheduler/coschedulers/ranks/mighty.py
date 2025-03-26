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
import pandas as pd

sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../../../../"
)))

from realsim.jobs.jobs import Job
from realsim.cluster.host import Host
from realsim.scheduler.coschedulers.ranks.ranks import RanksCoscheduler


class MightyCoscheduler(RanksCoscheduler, ABC):

    name = "Mighty Co-Scheduler"
    description = """Relational Co-scheduling with score"""
    #queue_depth = 100
    def __init__(self):
        super().__init__()
        self.queue_depth = 100
        self.score = 0

    def host_alloc_condition(self, hostname: str, job: Job) -> (float,float):

        return super().host_alloc_condition(hostname, job)

        """Condition on how to sort the hosts based on the speedup that the job
        will gain/lose. Always spread first
        """
        def get_job(name: int, jobs: list) -> Job:
            return next((j for j in jobs if j.job_name == name), None)
            
        def pairea(j1: Job, j2: Job=None) -> float:
            "Atomic function"
            area1 = j1.num_of_processes * j1.remaining_time

            if j2:
                area2 = j2.num_of_processes * j2.remaining_time
                s1 = self.database.heatmap[j1.job_name][j2.job_name]
                s2 = self.database.heatmap[j2.job_name][j1.job_name]

            else:
                area2 = 0
                s2 = 1 # does not matter
                s1 = j1.max_speedup
            
            return -(area1 / s1 + area2 / s2)

        # if empty node
        if self.cluster.hosts[hostname].state == Host.IDLE:
            return pairea(job, None)
            
        else:
            # get the jobs signatures that are assinged to the host
            co_job_sigs = list(self.cluster.hosts[hostname].jobs.keys())
            co_jobs = list(map(lambda sig: get_job(sig.split(":")[-1], self.cluster.execution_list), co_job_sigs))

            paireas = list(map(lambda j: pairea(job, j),co_jobs))
            return max(paireas)



    def deploy(self) -> bool:

        def calc_score(waiting_queue: list[Job]) -> float:
            # Calculate the score based on the waiting queue
            # For now, we will just return a fixed value
            weights = extract_weights(path, filters)
            weights['area'] = (weights['procs']*weights['time']/5).round().astype(int)

            queue = elise_df['Executable Number'].tolist()
            multi = elise_df['Executable Number'].value_counts().sort_index()
            data = pd.concat([weights, multi], axis=1, join='inner').fillna(0)
            probx = calc_probability(data)
            impactx = calc_impact(dictionary, probx)
            score = impactx*probx
            return float(score.sum().sum())

        deployed = False

        # Update the rank of each job before scheduling them
        # self.update_ranks()
        self.queue_depth = 100
        self.score = 0

        waiting_queue = deepcopy_list(self.cluster.waiting_queue[:self.queue_depth])
        waiting_queue.sort(key=lambda job: self.waiting_queue_reorder(job),
                           reverse=True)

        while waiting_queue != []:

            # calculate score
            score = calc_score(waiting_queue)

            # Remove from the waiting queue
            job = self.pop(waiting_queue)

            new_score = calc_score(waiting_queue)
            if new_score > score:
                alloc_policy = self.cluster.full_socket_allocation
            else:
                alloc_policy = self.cluster.half_socket_allocation

            # Colocate
            if self.allocation(job, alloc_policy):
                deployed = True
                self.after_deployment()
                break # that's the only addon
            else:
                break

        return deployed

    def backfill(self) -> bool:
        return super().backfill()

    def after_deployment(self, *args):
        return super().after_deployment(*args)
