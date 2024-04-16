import plotly.graph_objects as go
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from multiprocessing import Manager
from cProfile import Profile
import os
import sys

sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../"
)))

from realsim.cluster.exhaustive import ClusterExhaustive
from realsim.logger.logger import Logger


def run_sim(core):

    cluster, scheduler, logger, comm_queue = core

    cluster.setup()
    scheduler.setup()
    logger.setup()

    # The stopping condition is for the waiting queue and the execution list
    # to become empty
    while cluster.preloaded_queue != [] or cluster.waiting_queue != [] or cluster.execution_list != []:
        cluster.step()

    default_list = comm_queue.get()
    comm_queue.put(default_list)

    # When finished then use the default logger to get per job utilization
    # results
    default_cluster_makespan = default_list[0]
    default_logger = default_list[1]

    # if "Random" in scheduler.name:
    #     pr = Profile()
    #     pr.enable()

    data = {
            "Resource usage": logger.get_resource_usage(),
            "Jobs utilization": logger.get_jobs_utilization(default_logger),
            "Makespan speedup": default_cluster_makespan / cluster.makespan
    }

    # if "Random" in scheduler.name:
    #     pr.disable()
    #     pr.print_stats()


    # Return:
    # 1. Plot data for the resource usage in json format
    # 2. Jobs' utilization:
    #       a. Speedup for each job
    #       b. Turnaround ratio for each job
    #       c. Waiting time difference for each job
    # 3. Makespan speedup
    return data

class Simulation:
    """The entry point of a simulation for scheduling and scheduling algorithms.
    A user can decide whether the simulation will be 'static' be creating a bag
    of jobs at the beginning or 'dynamic' by continiously adding more jobs to
    the the waiting queue of a cluster.
    """

    def __init__(self, 
                 # generator bundle
                 jobs_set,
                 # cluster
                 nodes: int, ppn: int,
                 # scheduler algorithms bundled with inputs
                 schedulers_bundle):

        self.num_of_jobs = len(jobs_set)
        self.default = "Default Scheduler"
        self.executor = ProcessPoolExecutor()

        self.manager = Manager()
        self.comm_queue = self.manager.Queue(maxsize=1)

        self.sims = dict()
        self.futures = dict()
        self.results = dict()

        for sched_class, hyperparams in schedulers_bundle:

            # Setup cluster
            cluster = ClusterExhaustive(nodes, ppn)
            cluster.preload_jobs(jobs_set)

            # Setup scheduler
            scheduler = sched_class(**hyperparams)

            # Setup logger
            logger = Logger()

            # Setup experiment
            cluster.assign_scheduler(scheduler)
            scheduler.assign_cluster(cluster)
            cluster.assign_logger(logger)
            scheduler.assign_logger(logger)

            # The default simulation will be executed by the main process
            if sched_class.name == self.default:
                self.default_cluster = cluster
                self.default_scheduler = scheduler
                self.default_logger = logger
                continue

            # Record of a simulation
            self.sims[scheduler.name] = (cluster, 
                                         scheduler, 
                                         logger, 
                                         self.comm_queue)

    def set_default(self, name):
        # Set name for default scheduling algorithm
        self.default = name

    def run(self):

        # Fork out the workers
        for policy, sim in self.sims.items():
            print(policy, "submitted")
            self.futures[policy] = self.executor.submit(run_sim, sim)

        # Execute the default scheduler
        self.default_cluster.setup()
        self.default_scheduler.setup()
        self.default_logger.setup()

        # The stopping condition is for the waiting queue and the execution list
        # to become empty
        while self.default_cluster.preloaded_queue != [] or self.default_cluster.waiting_queue != [] or self.default_cluster.execution_list != []:
            self.default_cluster.step()

        # Submit to the shared list the results
        self.comm_queue.put([self.default_cluster.makespan, self.default_logger])

        # Wait until all the futures are complete
        self.executor.shutdown(wait=True)


    def get_results(self):

        # Set results for default scheduler
        data = {
                "Resource usage": self.default_logger.get_resource_usage(),
                "Jobs utilization": {},
                "Makespan speedup": 1.0
        }

        self.results[self.default] = data

        for policy, future in self.futures.items():

            # Get the results
            self.results[policy] = future.result()

        return self.results

