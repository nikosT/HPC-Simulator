from time import time
import plotly.graph_objects as go
from concurrent.futures import ThreadPoolExecutor
from queue import Queue
from functools import reduce
import os
import sys

sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../"
)))

from realsim.jobs import Job
from realsim.cluster.exhaustive import ClusterExhaustive
from realsim.logger.logger import Logger


def run_sim(core):

    sim_type, stop_condition, cluster, scheduler, logger, sets_hook, index_hook = core

    cluster.setup()
    scheduler.setup()
    logger.setup()

    # If the simulation is static
    if sim_type == "Static":

        # The stopping condition is for the waiting queue and the execution list
        # to become empty
        while cluster.waiting_queue != [] or cluster.execution_list != []:
            cluster.step()

    # If the simulation is dynamic
    elif sim_type == "Dynamic":

        condition = list(stop_condition.keys())[0]

        if condition == "Time":

            # These are real-world timers
            start_time = time()
            stop_time = stop_condition[condition]["Stop time"]

            # These are simulation based timers
            sim_timer = 0
            generator_timer = stop_condition[condition]["Generator time"]

            # The stopping condition will either not exist or it will be based on a
            # real world countdown
            while time() < start_time + stop_time:

                cluster.step()

                # Caclulate how much time passed for a step in order to deploy
                # the new batch(es) of jobs
                diff_time = cluster.makespan - (generator_timer + sim_timer)

                if diff_time >= 0:

                    print(f"[WORKER] INSIDE DIFF {scheduler.name} -> {cluster.makespan}")
                    # Calculate how many bags of should be consumed
                    bags = int(diff_time / generator_timer)
                    index = index_hook.get()
                    index += bags
                    index_hook.put(index)
                    print(scheduler.name, bags, index_hook)

                    # The execution is blocked until the new bags are delivered
                    for b in range(bags):
                        print(f"[WORKER] GET: {scheduler.name}: BEFORE")
                        jobs_set = sets_hook.get()
                        print(f"[WORKER] GET: {scheduler.name}: {jobs_set}")
                        # Each set of jobs arrived at different time
                        for job in jobs_set:
                            job.queued_time = sim_timer + (b + 1) * generator_timer
                        # Deploy to waiting queue
                        cluster.deploy_to_waiting_queue(jobs_set)


                    # Get the current time in the simulation
                    sim_timer = cluster.makespan

        else:
            pass

    return cluster, scheduler, logger

class Simulation:
    """The entry point of a simulation for scheduling and scheduling algorithms.
    A user can decide whether the simulation will be 'static' be creating a bag
    of jobs at the beginning or 'dynamic' by continiously adding more jobs to
    the the waiting queue of a cluster.
    """

    def __init__(self, 
                 # generator bundle
                 generator, gen_inp, simulation_type, stop_condition,
                 # cluster
                 nodes: int, ppn: int,
                 # scheduler algorithms bundled with inputs
                 schedulers_bundle):

        # self.num_of_jobs = len(inp)
        self.sim_type = simulation_type
        self.generator = generator
        self.gen_inp = gen_inp
        self.common_list = list()
        self.sets_hooks = dict()
        self.index_hooks = dict()
        self.executor = ThreadPoolExecutor(max_workers=len(schedulers_bundle))
        self.sims = dict()
        self.futures = dict()
        self.results = dict()

        start_jobs_set = generator.generate_jobs_set(gen_inp)
        self.common_list.append(start_jobs_set)

        for sched_class, hyperparams in schedulers_bundle:

            # Setup cluster
            cluster = ClusterExhaustive(nodes, ppn)
            cluster.deploy_to_waiting_queue(start_jobs_set)

            # Setup scheduler
            scheduler = sched_class(**hyperparams)

            # Setup logger
            logger = Logger()

            # Setup experiment
            cluster.assign_scheduler(scheduler)
            scheduler.assign_cluster(cluster)
            cluster.assign_logger(logger)
            scheduler.assign_logger(logger)

            # Sets of jobs hook
            sets_hook = Queue()
            self.sets_hooks[scheduler.name] = sets_hook

            # Index hook
            index_hook = Queue(maxsize=1) # store only a number
            index_hook.put(1) # one because the first set of jobs was deployed
            self.index_hooks[scheduler.name] = index_hook

            # Record of a simulation
            self.sims[scheduler.name] = (simulation_type,
                                         stop_condition,
                                         cluster, 
                                         scheduler, 
                                         logger,
                                         sets_hook,
                                         index_hook)

    def set_default(self, name):
        self.default = name

    def run(self):
        for policy, sim in self.sims.items():
            print(policy, "submitted")
            self.futures[policy] = self.executor.submit(run_sim, sim)

        # Is any task still running?
        running = sum(map(lambda future: 1 if future.running() else 0,
                          self.futures.values()))
        
        while running:

            # Find maximum number of needed bags
            indices = []
            for index_hook in self.index_hooks.values():
                try:
                    value = index_hook.get(block=False)
                except:
                    value = 0
                indices.append(value)
                index_hook.put(value)

            max_index = max(indices)

            # Difference between length of common_list and max index
            diff = max_index - len(self.common_list)

            print(f"[MAIN] MAX INDEX = {max_index}")
            # If it is greater then append to common_list and put to the queues
            if diff > 0:
                print("[MAIN] INSIDE DIFF")
                for _ in range(diff):
                    jobs_set = self.generator.generate_jobs_set(self.gen_inp)
                    self.common_list.append(jobs_set)
                    for queue in self.sets_hooks.values():
                        queue.put(jobs_set)
            # If it is not then pass
            else:
                pass
        
            # Check if any task is still running
            running = sum(map(lambda future: 1 if future.running() else 0,
                              self.futures.values()))

    def plot(self):

        figures = dict()

        # Wait until all the futures are complete
        self.executor.shutdown(wait=True)

        for policy, future in self.futures.items():

            # Get the results
            self.results[policy] = future.result()

            # Plot resource usage
            logger = self.results[policy][2]
            print(logger.cluster_events)
            fig = logger.get_resource_usage(save=False)
            figures[f"Plot Resources: {policy}"] = fig

            fig.show()

        # speedups = list() # makespan speedups
        # boxpoints = list()
        # compact_logger = self.results[self.default][2]

        # policies = list( self.sims.keys() )
        # policies.remove(self.default)

        # for policy in sorted(policies):
        #     logger = self.results[policy][2]
        #     speedups.append(
        #             self.results[self.default][0].makespan / self.results[policy][0].makespan
        #     )
        #     boxpoints.append( logger.get_jobs_utilization(compact_logger) )

        # fig = go.Figure()

        # for i, points in enumerate(boxpoints):
        #     names = list()
        #     s_values = list()
        #     t_values = list()
        #     for name, value in points.items():
        #         names.append(name)
        #         s_values.append(value["speedup"])
        #         t_values.append(value["turnaround"])

        #     fig.add_trace(
        #             go.Box(
        #                 y=s_values,
        #                 x=[i]*len(points),
        #                 name="speedup",
        #                 boxpoints="all",
        #                 boxmean="sd",
        #                 text=names,
        #                 marker_color="red",
        #                 showlegend=False
        #             )
        #     )

        # fig.add_trace(
        #         go.Scatter(x=list(range(len(speedups))), 
        #                    y=speedups, mode="lines+markers+text",
        #                    marker=dict(color="black"), name="Makespan Speedup"
        #         )
        # )

        # for x in range(len(speedups)):
        #     fig.add_annotation(text=f"<b>{round(speedups[x], 3)}</b>",
        #                        x=x,
        #                        y=speedups[x],
        #                        arrowcolor="black")


        # fig.add_hline(y=1, line_color="black", line_dash="dot")

        # fig.update_layout(
        #         title=f"<b>Makespan and per job speedups for {self.num_of_jobs} jobs</b>",
        #         title_x=0.5,
        #         # height=1080,
        #         # width=1920,
        #         xaxis=dict(
        #             title="<b>Co-Schedulers</b>",
        #             tickmode="array",
        #             tickvals=[x for x in range(len(policies))],
        #             ticktext=sorted(policies)
        #         ),
        #         yaxis=dict(title="<b>Speedup</b>"),
        #         template="seaborn"
        # )

        # figures["Speedups"] = fig

        # return figures


