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
from realsim.cluster.host import Host
from realsim.scheduler.coschedulers.ranks.ranks import RanksCoscheduler


class UtilCoscheduler3(RanksCoscheduler, ABC):

    name = "Util Co-Scheduler3"
    description = """Co-scheduling favoring Utilization filling"""

    def waiting_queue_reorder(self, job: Job) -> float:

        def age():
            """
            score e [0,1]: 1: is better
            """
            _list = list(map(lambda j: j.waiting_time, self.cluster.waiting_queue[:self.queue_depth]))
            if not _list:
                return 0
            else:
                max_waiting_time = max(_list)

            if not max_waiting_time:
                return 0
            else:
                return job.waiting_time / max_waiting_time

        def frag():
            """
            score e [0,1]: 1: is better
            """
            free = self.cluster.get_idle_cores()
            if not free:
                return -1
            if job.num_of_processes > free:
                return -1
            return job.num_of_processes/free

        w_age = 0.5
        w_frag = 0.5

        wmean = w_age * age() + w_frag * frag()

        return wmean


        def pairea(self, job):
            """
            NOT USED
            """
            score_vector = list(map(lambda h: self.host_alloc_condition(h, job), self.cluster.hosts.keys()))
            idle_cores_vector = list(map(lambda h: self.cluster.hosts[h].get_idle_cores_num(), self.cluster.hosts.keys()))
            
            vector = list(zip(score_vector, idle_cores_vector))  # Correctly zipping the two lists
            vector.sort(key=lambda x: x[0], reverse=True)  # Sorting based on the first value (score)

            _sum = 0
            filter_vector = []
            for x in vector:
                if _sum + x[0] <= job.num_of_processes:
                    filter_vector.append(x)
                    _sum += x[0]
                else:
                    break  # Stop if exceeding job.num_of_processes

            if not filter_vector:
                return 0  # Edge case: avoid min() on an empty list

            score = min(x[0] for x in filter_vector)  # Minimum score from filtered vector
            return score

    def host_alloc_condition(self, hostname: str, job: Job) -> (float,float):
        """Condition on how to sort the hosts based on the speedup that the job
        will gain/lose. Always spread first
        """
        def get_job(name: int, jobs: list) -> Job:
            return next((j for j in jobs if j.job_name == name), None)
            
        def pairea(j1: Job, j2: Job=None) -> float:
            "Atomic function"

            if j2:
                s1 = self.database.heatmap[j1.job_name][j2.job_name]
                s2 = self.database.heatmap[j2.job_name][j1.job_name]

                if j2.remaining_time <= j1.remaining_time:
                    score = j1.num_of_processes * ( (j2.remaining_time/s2)*(1-s1/j1.avg_speedup) + j1.remaining_time/j1.avg_speedup ) + j2.num_of_processes * (j2.remaining_time / s2)
                else:
                    score = j2.num_of_processes * ( (j1.remaining_time/s1)*(1-s2/j2.avg_speedup) + j2.remaining_time/j2.avg_speedup ) + j1.num_of_processes * (j1.remaining_time / s1)
                return (j1.remaining_time * j1.num_of_processes + j2.remaining_time * j2.num_of_processes - score ) * (self.cluster.hosts[hostname].get_idle_cores_num()/j1.num_of_processes)

            else:
                area1 = j1.num_of_processes * j1.remaining_time
                s1 = j1.max_speedup
                return (area1 - (area1/s1) ) * (self.cluster.hosts[hostname].get_idle_cores_num()/j1.num_of_processes)

        #return self.cluster.hosts[hostname].get_used_cores_num()

        # if empty node
        if self.cluster.hosts[hostname].state == Host.IDLE:
            return pairea(job, None)
            
        else:
            # get the jobs signatures that are assinged to the host
            co_job_sigs = list(self.cluster.hosts[hostname].jobs.keys())
            co_jobs = list(map(lambda sig: get_job(sig.split(":")[-1], self.cluster.execution_list), co_job_sigs))

            paireas = list(map(lambda j: pairea(job, j),co_jobs))
            return max(paireas)

        #return super().host_alloc_condition(hostname, job)

    def deploy(self) -> bool:
        self.queue_depth = None # 10

        deployed = False

        # Update the rank of each job before scheduling them
        # self.update_ranks()

        waiting_queue = deepcopy_list(self.cluster.waiting_queue[:self.queue_depth])
        waiting_queue.sort(key=lambda job: self.waiting_queue_reorder(job),
                           reverse=True)

        while waiting_queue != []:

            # Remove from the waiting queue
            job = self.pop(waiting_queue)

            # Colocate
            if self.allocation(job, self.cluster.half_socket_allocation):
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
