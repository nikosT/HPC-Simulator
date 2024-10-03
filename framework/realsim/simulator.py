from realsim.logger.logger import Logger
from realsim.cluster.exhaustive import ClusterExhaustive
from realsim.database import Database
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from multiprocessing import Manager
import os
import sys
import math
from cProfile import Profile
import pstats
import io
import datetime

sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../"
)))


def run_sim(core):

    database, cluster, scheduler, logger, comm_queue = core

    cluster.setup()
    scheduler.setup()
    logger.setup()

    # The stopping condition is for the waiting queue and the execution list
    # to become empty
    while database.preloaded_queue != [] or cluster.waiting_queue != [] or cluster.execution_list != []:
        cluster.step()

    default_list = comm_queue.get()
    comm_queue.put(default_list)

    # When finished then use the default logger to get per job utilization
    # results
    default_cluster_makespan = default_list[0]
    default_logger = default_list[1]

    # profiler = Profile()
    # profiler.enable()

    data = {
        # Graphs
        # "Resource usage": logger.get_resource_usage(),
        "Gantt diagram": logger.get_gantt_representation(),
        "Unused cores": logger.get_unused_cores_graph(),
        "Jobs utilization": logger.get_jobs_utilization(default_logger),
        "Jobs throughput": logger.get_jobs_throughput(),
        "Waiting queue": logger.get_waiting_queue_graph(),
        "Workload": logger.get_workload(),

        # Extra metrics
        "Makespan speedup": default_cluster_makespan / cluster.makespan
    }


    _csvpath = f'/home/nikos/Desktop/ipdps2025/thanos/dummy_workloads/experiment_{scheduler.name}_{datetime.datetime.now().strftime("%Y%m%d%H%M%S%f")}.csv'
    with open (_csvpath,'w') as _f:
        _f.write(data["Workload"])

    # profiler.disable()
    # stats = pstats.Stats(profiler).sort_stats('cumtime')
    # stats.print_stats(30)
    # with open("out.txt", "w") as fd:
    #     fd.write(s.getvalue())
    # stats.print_stats(15)

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
                 jobs_set, heatmap,
                 # cluster
                 nodes: int, ppn: int, queue_size: int,
                 # scheduler algorithms bundled with inputs
                 schedulers_bundle):

        self.default = "FIFO Scheduler"
        self.executor = ProcessPoolExecutor()

        self.manager = Manager()
        self.comm_queue = self.manager.Queue(maxsize=1)

        self.sims = dict()
        self.futures = dict()
        self.results = dict()

        for sched_class, hyperparams in schedulers_bundle:

            # Declare a database for each simulation step
            database = Database(jobs_set, heatmap)

            # Declare cluster
            cluster = ClusterExhaustive(nodes, ppn)
            cluster.assign_database(database)

            # Define the size of the queue for the cluster
            if queue_size == -1:
                cluster.queue_size = math.inf
            else:
                cluster.queue_size = queue_size

            # Setup the databse
            database.setup()
            cluster.setup_preloaded_jobs()

            # Setup scheduler
            scheduler = sched_class(**hyperparams)
            scheduler.assign_database(database)

            # Setup logger
            logger = Logger()
            logger.assign_database(database)

            # Setup experiment
            cluster.assign_scheduler(scheduler)
            scheduler.assign_cluster(cluster)
            cluster.assign_logger(logger)
            scheduler.assign_logger(logger)

            # The default simulation will be executed by the main process
            if sched_class.name == self.default:
                self.default_database = database
                self.default_cluster = cluster
                self.default_scheduler = scheduler
                self.default_logger = logger
                continue

            # Record of a simulation
            self.sims[scheduler.name] = (database,
                                         cluster,
                                         scheduler,
                                         logger,
                                         self.comm_queue)

    def set_default(self, name):
        # Set name for default scheduling algorithm
        self.default = name

    def run(self):

        # Fork out the workers
        for policy, sim_args in self.sims.items():
            print(policy, "submitted")
            self.futures[policy] = self.executor.submit(run_sim, sim_args)

        print("Running the default scheduler")

        # Execute the default scheduler
        self.default_cluster.setup()
        self.default_scheduler.setup()
        self.default_logger.setup()

        # The stopping condition is for the waiting queue and the execution list
        # to become empty
        while self.default_database.preloaded_queue != [] or self.default_cluster.waiting_queue != [] or self.default_cluster.execution_list != []:
            self.default_cluster.step()

        # Submit to the shared list the results
        self.comm_queue.put(
            [self.default_cluster.makespan, self.default_logger])

        # Wait until all the futures are complete
        self.executor.shutdown(wait=True)

    def get_results(self):

        # Set results for default scheduler
        data = {
            # "Resource usage": self.default_logger.get_resource_usage(),
            "Gantt diagram": self.default_logger.get_gantt_representation(),
            "Unused cores": self.default_logger.get_unused_cores_graph(),
            "Jobs utilization": {},
            "Makespan speedup": 1.0,
            "Jobs throughput": self.default_logger.get_jobs_throughput(),
            "Workload": self.default_logger.get_workload(),
            "Waiting queue": self.default_logger.get_waiting_queue_graph(),
        }

        self.results[self.default] = data

        for policy, future in self.futures.items():

            # Get the results
            self.results[policy] = future.result()

        return self.results
