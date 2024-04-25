from .scheduler import Scheduler
from realsim.jobs.utils import deepcopy_list


class CompactScheduler(Scheduler):

    name = "Default Scheduler"
    description = "No co-location policy; or default policy"

    def __init__(self, backfill_enabled: bool = True):
        self.backfill_enabled = backfill_enabled
        Scheduler.__init__(self)

    def setup(self):
        pass

    def deploy(self) -> bool:

        # Did we deploy any job?
        deployed = False

        waiting_queue = deepcopy_list(self.cluster.waiting_queue)

        while waiting_queue != []:

            job = self.pop(waiting_queue)

            if self.cluster.full_node_cores(job) <= self.cluster.free_cores:
                self.cluster.waiting_queue.remove(job)
                job.binded_cores = self.cluster.full_node_cores(job)
                self.cluster.execution_list.append([job])
                self.cluster.free_cores -= job.binded_cores
                deployed = True

        return deployed

