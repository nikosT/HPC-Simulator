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

        # Get compute nodes that can be reserved for the execution of jobs
        free_compute_nodes = [
                compute_node 
                for compute_node in self.cluster.compute_nodes
                if len(compute_node) == self.cluster.cores_per_node
        ]

        waiting_queue = deepcopy_list(self.cluster.waiting_queue)

        while waiting_queue != []:

            job = self.pop(waiting_queue)

            # If there are not any compute nodes left for the job exit
            if len(free_compute_nodes) < job.full_nodes:
                break

            # Reserve nodes for execution
            reserve_nodes = free_compute_nodes[:job.full_nodes]

            for node in reserve_nodes:
                job.reserved_nodes |= node
                self.cluster.free_cores -= node
                free_compute_nodes.remove(node)

            self.cluster.waiting_queue.remove(job)
            self.cluster.execution_list.append([job])
            job.start_time = self.cluster.makespan
            job.binded_cores = job.full_node_cores
