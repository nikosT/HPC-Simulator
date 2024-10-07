from abc import ABC
from .ranks import RanksCoscheduler
from numpy.random import seed, randint
from time import time_ns
import os
import sys
from math import inf

sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../../../../"
)))

from realsim.jobs.jobs import Job
from realsim.scheduler.coschedulers.ranks.ranks import RanksCoscheduler
from realsim.cluster.host import Host


class BesterCoscheduler(RanksCoscheduler, ABC):

    name = "Bester Ranks Co-Scheduler"
    description = """Random co-scheduling using ranks architecture as a fallback
    to classic scheduling algorithms"""

    def waiting_queue_reorder(self, job: Job) -> float:
        # seed(time_ns() % (2 ** 32))
        # return float(randint(len(self.cluster.waiting_queue)))
	    return 1.0

    def coloc_condition(self, hostname: str, job: Job):

        co_job_sigs = list(self.cluster.hosts[hostname].jobs.keys())

        if co_job_sigs == []:
            return (inf, inf)

        co_job = None
        for xjob in self.cluster.execution_list:
            if xjob.get_signature() == co_job_sigs[0]:
                co_job = xjob

        if co_job is None:
            return (0, 0)

        points = 0
        if job.half_socket_nodes == co_job.half_socket_nodes:
            points += 1

        estimated_rem_time = (co_job.start_time + co_job.wall_time) - self.cluster.makespan
        if abs(job.wall_time - estimated_rem_time) / estimated_rem_time < 0.2:
            points += 1

        sp1 = self.database.heatmap[job.job_name][co_job.job_name]
        sp2 = self.database.heatmap[co_job.job_name][job.job_name]
        if sp1 is None or sp2 is None:
            return (points, job.avg_speedup)
        avg_sp = (sp1 + sp2) / 2

        if avg_sp >= 1:
            points += 1

        return (points, avg_sp)

