from .scheduler import Scheduler
from realsim.jobs.utils import deepcopy_list


class FIFOScheduler(Scheduler):

    name = "FIFO Scheduler"
    description = "First In First Out/ First Come First Served scheduling policy"

    def __init__(self):
        Scheduler.__init__(self)

    def setup(self):
        pass

    def deploy(self) -> None:

        waiting_queue = deepcopy_list(self.cluster.waiting_queue)

        while waiting_queue != []:

            job = self.pop(waiting_queue)

            if self.cluster.full_node_cores(job) <= self.cluster.free_cores:
                self.cluster.waiting_queue.remove(job)
                job.binded_cores = self.cluster.full_node_cores(job)
                self.cluster.execution_list.append([job])
                job.start_time = self.cluster.makespan
                self.cluster.free_cores -= job.binded_cores
            else:
                break

