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


class BudgetCoscheduler(RanksCoscheduler, ABC):

    name = "Budget Co-Scheduler"
    description = """Co-scheduling favoring budget"""


    def host_alloc_condition(self, hostname: str, job: Job) -> (float,float):
        """Condition on how to sort the hosts with best-fit approach
        """
        def get_job(name: int, jobs: list) -> Job:
             return next((j for j in jobs if j.job_name == name), None)
        
        host = self.cluster.hosts[hostname]

        if host.state == Host.IDLE:
            return 0, 0
        
        # find the jobs that run in the host
        # get the jobs signatures that are assinged to the host
        co_job_sigs = list(host.jobs.keys())
        co_jobs = list(map(lambda sig: get_job(sig.split(":")[-1], self.cluster.execution_list), co_job_sigs))

        # find the time when these jobs will finish
        co_job = max(co_jobs, key=lambda j: j.remaining_time * self.database.heatmap[j.job_name][job.job_name])

        sjob = self.database.heatmap[job.job_name][co_job.job_name]
        scojob = self.database.heatmap[co_job.job_name][job.job_name]
        max_remaining_time = co_job.remaining_time * scojob

        # find the time when the job will finish

        return host.get_idle_cores_num(), -abs(job.remaining_time*sjob - max_remaining_time)


        # return super().host_alloc_condition(hostname, job)

        # def get_job(name: int, jobs: list) -> Job:
        #     return next((j for j in jobs if j.job_name == name), None)
            
        # def pairea(j1: Job, j2: Job=None) -> float:
        #     "Atomic function"

        #     if j2:
        #         s1 = self.database.heatmap[j1.job_name][j2.job_name]
        #         s2 = self.database.heatmap[j2.job_name][j1.job_name]

        #         if j2.remaining_time <= j1.remaining_time:
        #             score = j1.num_of_processes * ( (j2.remaining_time/s2)*(1-s1/j1.avg_speedup) + j1.remaining_time/j1.avg_speedup ) + j2.num_of_processes * (j2.remaining_time / s2)
        #         else:
        #             score = j2.num_of_processes * ( (j1.remaining_time/s1)*(1-s2/j2.avg_speedup) + j2.remaining_time/j2.avg_speedup ) + j1.num_of_processes * (j1.remaining_time / s1)
        #         return (j1.remaining_time * j1.num_of_processes + j2.remaining_time * j2.num_of_processes - score ) * (self.cluster.hosts[hostname].get_idle_cores_num()/j1.num_of_processes)

        #     else:
        #         area1 = j1.num_of_processes * j1.remaining_time
        #         s1 = j1.max_speedup
        #         return (area1 - (area1/s1) ) * (self.cluster.hosts[hostname].get_idle_cores_num()/j1.num_of_processes)

        # #return self.cluster.hosts[hostname].get_used_cores_num()

        # # if empty node
        # if self.cluster.hosts[hostname].state == Host.IDLE:
        #     return pairea(job, None)
            
        # else:
        #     # get the jobs signatures that are assinged to the host
        #     co_job_sigs = list(self.cluster.hosts[hostname].jobs.keys())
        #     co_jobs = list(map(lambda sig: get_job(sig.split(":")[-1], self.cluster.execution_list), co_job_sigs))

        #     paireas = list(map(lambda j: pairea(job, j),co_jobs))
        #     return max(paireas)

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
            elif self.allocation(job, self.cluster.full_socket_allocation):
                deployed = True
                self.after_deployment()
                break
            else:
                break

        return deployed

    def backfill(self) -> bool:
        return super().backfill()

    def after_deployment(self, *args):
        return super().after_deployment(*args)
