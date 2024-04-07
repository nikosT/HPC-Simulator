from typing import TypeVar
import plotly.graph_objects as go
from concurrent.futures import ThreadPoolExecutor
import os
import sys

sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../"
)))

from realsim.jobs import Job
from realsim.cluster.exhaustive import ClusterExhaustive
from realsim.scheduler.balancerFullOn import BalancerFullOn
from realsim.scheduler.compact import CompactScheduler
from realsim.scheduler.random import RandomScheduler
from realsim.logger.logger import Logger

Scheduler = TypeVar("Scheduler", CompactScheduler, RandomScheduler, BalancerFullOn)

def run_sim(core):
    cluster, scheduler, logger = core
    cluster.run()
    return cluster, scheduler, logger

class Experiment:

    def __init__(self, 
                 # set of jobs
                 inp: list[Job],
                 # cluster
                 nodes: int, ppn: int,
                 # scheduler algorithms
                 schedulers):

        self.num_of_jobs = len(inp)
        self.executor = ThreadPoolExecutor(max_workers=len(schedulers))
        self.sims = dict()
        self.futures = dict()
        self.results = dict()

        for scheduler_class in schedulers:

            # Setup cluster
            cluster = ClusterExhaustive(nodes, ppn)
            cluster.deploy_to_waiting_queue(inp)

            # Setup scheduler
            scheduler = scheduler_class()

            # Setup logger
            logger = Logger()

            # Setup experiment
            cluster.assign_scheduler(scheduler)
            scheduler.assign_cluster(cluster)
            cluster.assign_logger(logger)
            scheduler.assign_logger(logger)

            self.sims[scheduler.name] = (cluster, scheduler, logger)

    def set_default(self, name):
        self.default = name

    def run(self):
        for policy, sim in self.sims.items():
            print(policy, "submitted")
            self.futures[policy] = self.executor.submit(run_sim, sim)

    def plot(self):

        figures = dict()

        # Wait until all the futures are complete
        self.executor.shutdown(wait=True)

        for policy, future in self.futures.items():

            # Get the results
            self.results[policy] = future.result()

            # Plot resource usage
            logger = self.results[policy][2]
            figures[f"Plot Resources: {policy}"] = logger.get_resource_usage(save=False)

        speedups = list() # makespan speedups
        boxpoints = list()
        compact_logger = self.results[self.default][2]

        policies = list( self.sims.keys() )
        policies.remove(self.default)

        for policy in sorted(policies):
            logger = self.results[policy][2]
            speedups.append(
                    self.results[self.default][0].makespan / self.results[policy][0].makespan
            )
            boxpoints.append( logger.get_jobs_utilization(compact_logger) )

        fig = go.Figure()

        for i, points in enumerate(boxpoints):
            names = list()
            s_values = list()
            t_values = list()
            for name, value in points.items():
                names.append(name)
                s_values.append(value["speedup"])
                t_values.append(value["turnaround"])

            fig.add_trace(
                    go.Box(
                        y=s_values,
                        x=[i]*len(points),
                        name="speedup",
                        boxpoints="all",
                        boxmean="sd",
                        text=names,
                        marker_color="red",
                        showlegend=False
                    )
            )

        fig.add_trace(
                go.Scatter(x=list(range(len(speedups))), 
                           y=speedups, mode="lines+markers+text",
                           marker=dict(color="black"), name="Makespan Speedup"
                )
        )

        for x in range(len(speedups)):
            fig.add_annotation(text=f"<b>{round(speedups[x], 3)}</b>",
                               x=x,
                               y=speedups[x],
                               arrowcolor="black")


        fig.add_hline(y=1, line_color="black", line_dash="dot")

        fig.update_layout(
                title=f"<b>Makespan and per job speedups for {self.num_of_jobs} jobs</b>",
                title_x=0.5,
                # height=1080,
                # width=1920,
                xaxis=dict(
                    title="<b>Co-Schedulers</b>",
                    tickmode="array",
                    tickvals=[x for x in range(len(policies))],
                    ticktext=sorted(policies)
                ),
                yaxis=dict(title="<b>Speedup</b>"),
                template="seaborn"
        )

        figures["Speedups"] = fig

        return figures
        #fig.show()
        # fig.write_image(f"resources_usage/boxplots.pdf", format="pdf")


