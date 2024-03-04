import os
import sys

sys.path.append(os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "../../../"
        )
    ))

from typing import Dict, TYPE_CHECKING
import re
#if TYPE_CHECKING:
#    from realsim.cluster import ClusterV2
from realsim.jobs.jobs import EmptyJob, Job
import plotly.graph_objects as go
import plotly.express.colors as colors
from numpy import average as avg
from numpy import median

class Logger(object):
    """
    Logs important events of the simulation into the memory
    for later use
    """

    def __init__(self, recording=True):
        # Controls if the events will be logged
        self.recording = recording
        self.scale = colors.sequential.Turbo

    def assign_cluster(self, cluster):
        self.cluster = cluster

    def init_logger(self):

        # Cluster wide events
        self.cluster_events = dict()
        self.cluster_events["checkpoints"] = set()
        self.cluster_events["deploying:exec-colocation"] = 0
        self.cluster_events["deploying:wait-colocation"] = 0
        self.cluster_events["deploying:compact"] = 0
        self.cluster_events["deploying:success"] = 0
        self.cluster_events["deploying:failed"] = 0

        # Events #
        # Job events
        self.job_events: Dict[str, Dict] = dict()

        # Init job events
        for job in self.cluster.waiting_queue:
            # Job events
            jevts = {
                    "runtime": [], # [co-job, start time, end time]
                    "speedups": [], # [sp1, sp2, ..]
                    "cores": dict(), # {cojob1: cores1, cojob2: cores2, ..}
                    "remaining time": []
                    }
            self.job_events[f"{job.job_id}:{job.job_name}"] = jevts

    def cluster_deploying_exec(self):
        pass
    
    def cluster_deploying_wait(self):
        pass

    def cluster_deploying_compact(self):
        pass

    def cluster_deploying_success(self):
        pass

    def cluster_deploying_fail(self):
        pass

    def jobs_start(self):

        self.cluster_events["checkpoints"].add(
                self.cluster.makespan
        )

        for item in self.cluster.execution_list:

            if len(item) == 1 and type(item[0]) != EmptyJob:

                job_key = f"{item[0].job_id}:{item[0].job_name}"

                # Get the starting time of job for each checkpoint
                self.job_events[job_key]["runtime"].append(
                        ["compact", self.cluster.makespan, None]
                )

                # Set speedup of job for checkpoint to 1 as it 
                # is compact executing
                self.job_events[job_key]["speedups"].append(item[0].speedup)

                # Get compact cores usage
                self.job_events[job_key]["cores"].update({
                    "compact": item[0].binded_cores
                })

                self.job_events[job_key]["remaining time"].append(
                        item[0].remaining_time
                )

            elif len(item) > 1:
                # Find if every job has finished
                empty = True
                for job in item:
                    if type(job) != EmptyJob:
                        empty = False
                        break
                
                if empty:
                    continue

                head_key = f"{item[0].job_id}:{item[0].job_name}"

                # Get tail key
                tail_key = "spread"
                tail = [x for x in item[1:] if type(x) != EmptyJob ]

                if tail != []:

                    tail_key = "|".join([
                        f"{y.job_id}:{y.job_name}" for y in tail
                        if type(y) != EmptyJob
                    ])

                    for job in tail:

                        job_key = f"{job.job_id}:{job.job_name}"

                        self.job_events[job_key]["runtime"].append(
                                [head_key, self.cluster.makespan, None]
                        )

                        self.job_events[job_key]["speedups"].append(
                                job.speedup
                        )

                        self.job_events[job_key]["cores"].update({
                            head_key: job.binded_cores
                        })

                        self.job_events[job_key]["remaining time"].append(
                                job.remaining_time
                        )

                self.job_events[head_key]["runtime"].append(
                        [tail_key, self.cluster.makespan, None]
                )
                
                self.job_events[head_key]["speedups"].append(item[0].speedup)

                self.job_events[head_key]["cores"].update({
                    tail_key: item[0].binded_cores
                })

                self.job_events[head_key]["remaining time"].append(
                        item[0].remaining_time
                )
            else:
                pass

    def job_finish(self, job: Job):
        job_key = f"{job.job_id}:{job.job_name}"
        self.job_events[job_key]["runtime"][-1][-1] = self.cluster.makespan

        self.cluster_events["checkpoints"].add(
                self.cluster.makespan
        )

    def timeline_data(self):

        # job_key: {cojob_key: [start time, end time]}, {cojob_key2: [st2, et2]}

        data = dict()

        for job_key, job_event in self.job_events.items():

            runtimes = job_event["runtime"]

            # Some runtimes lists will be empty
            # because we didn't want to save information twice
            if runtimes == []:
                continue

            # Initialize a dictionary for each non empty job key
            data[job_key] = dict()

            current_cojob_key = runtimes[0][0]
            current_starting_time = runtimes[0][1]

            if current_cojob_key in data and job_key in data[current_cojob_key]:
                # Should I check it?
                pass

            # Swipe through the whole runtime list
            if len(runtimes) > 1:

                for rtime in runtimes[1:]:

                    if current_cojob_key != rtime[0]:

                        data[job_key].update({
                            current_cojob_key: [current_starting_time, rtime[1]]
                        })

                        current_cojob_key = rtime[0]
                        current_starting_time = rtime[1]

                    data[job_key].update({
                        current_cojob_key: [current_starting_time, rtime[2]]
                    })

            else:

                data[job_key].update({
                    current_cojob_key: [current_starting_time, runtimes[0][2]]
                })

        return data

    def plot_resources(self, save=False):

        checks = list(sorted(
                self.cluster_events["checkpoints"]
        ))

        intervals = [b - a for a, b in list(zip(
            checks[:len(checks)], checks[1:]
        ))]

        timeline_data = self.timeline_data()

        num_of_jobs = len(timeline_data.keys())
        
        jcolors = colors.sample_colorscale(self.scale, [n/(num_of_jobs - 1) for n in range(num_of_jobs)])

        traces = list()
        for idx, [job_key, val] in enumerate(timeline_data.items()):

            for cojob_key, times in val.items():

                job_cores = self.job_events[job_key]["cores"][cojob_key]
                empty_space = 0
                if cojob_key != "spread" and cojob_key != "compact":
                    
                    lesser_cores = sum([
                        self.job_events[cj_key]["cores"][job_key]
                        for cj_key in cojob_key.split("|")
                        if job_key in self.job_events[cj_key]["cores"].keys()
                    ])

                    if lesser_cores > 0 and lesser_cores < job_cores:
                        empty_space = job_cores - lesser_cores

                elif cojob_key == "spread":
                    empty_space = job_cores

                xs = [
                    checks[i] + intervals[i] / 2
                    for i, time in enumerate(checks[:len(checks)])
                    if time >= times[0] and time < times[1]
                ]

                ws = [
                    intervals[i]
                    for i, time in enumerate(checks[:len(checks)])
                    if time >= times[0] and time < times[1]
                ]

                ys = [self.job_events[job_key]["cores"][cojob_key]] * len(xs)

                #xs = [times[0] + (times[1] - times[0]) / 2]
                #ws = [times[1] - times[0]]
                #ys = [self.job_events[job_key]["cores"][cojob_key]] * len(xs)

                if cojob_key == "compact":
                    text = f"<b>{job_key}<br>[{cojob_key}]</b><br>nodes = {int(self.job_events[job_key]['cores'][cojob_key] / self.cluster.cores_per_node)}"
                else:
                    text = f"<b>{job_key}<br>[{cojob_key}]</b><br>nodes = {int(2 * self.job_events[job_key]['cores'][cojob_key] / self.cluster.cores_per_node)}"

                traces.append(
                        go.Bar(
                            x=xs,
                            y=ys,
                            width=ws,
                            name=f"{job_key}_{cojob_key}",
                            text=text,
                            insidetextanchor="middle",
                            marker_line=dict(width=2, color="black"),
                            marker=dict(
                                color=jcolors[idx]
                            )
                        )
                )

                if empty_space > 0:

                    traces.append(
                            go.Bar(
                                x=xs,
                                y=[empty_space] * len(xs),
                                width=ws,
                                name=f"{job_key.split(':')[0]}:empty",
                                text="binded",
                                insidetextanchor="middle",
                                marker_line=dict(width=2, color="black"),
                                marker=dict(
                                    color="black",
                                    opacity=1
                                )
                            )
                    )

        fig = go.Figure(data=traces)

        for check in checks:
            fig.add_vline(x=check, line_width=0.5, line_dash="dot",)

        fig.update_layout(
                title=f"<b>{self.cluster.scheduler.name}</b>",
                title_x=0.5,
                #height=1080,
                #width=1920,
                showlegend=True,
                yaxis=dict(
                    title="<b>Cores</b>",
                    range=[0, self.cluster.total_cores],
                    tickmode="array",
                    tickvals=[self.cluster.total_cores],
                ),
                xaxis=dict(
                    title="<b>Time</b>",
                    tickmode="array",
                    tickvals=checks,
                    ticktext=[f"{t:.3f}" for t in checks]
                ),
                barmode="stack",
                autosize=True,
                bargap=0.1
        )

        if save:
            if not os.path.exists("./resources_usage"):
                os.mkdir("./resources_usage")
            fig.update_layout(showlegend=False)
            fig.write_image(f"resources_usage/{self.cluster.scheduler.name}.chart.pdf",
                            format="pdf")
        else:
            pass
            #fig.show()

        return fig

    def jobs_speedup_boxpoints(self, logger):

        if not isinstance(logger, Logger):
            raise Exception("Provide a logger")

        # Boxplot points
        points = list()

        # Get the time spent on a job in both loggers
        our_timeline = self.timeline_data()
        their_timeline = logger.timeline_data()

        for job_key in our_timeline:

            # Get all the times in our job_key
            our_job_times = list()
            for _, timeline in our_timeline[job_key].items():
                our_job_times.extend(timeline)

            # Get our job execution time
            our_job_interval = max(our_job_times) - min(our_job_times)

            # Get all the times in their job_key
            their_job_times = list()
            for _, timeline in their_timeline[job_key].items():
                their_job_times.extend(timeline)

            # Get our job execution time
            their_job_interval = max(their_job_times) - min(their_job_times)

            points.append(their_job_interval / our_job_interval)

            # if their_job_interval / our_job_interval < 0.8:
            #     print(job_key, self.job_events[job_key]["remaining time"])

        return points

    def jobs_boxpoints(self, logger):

        if not isinstance(logger, Logger):
            raise Exception("Provide a logger")

        # Boxplot points
        points = dict()

        # Get the time spent on a job in both loggers
        our_timeline = self.timeline_data()
        their_timeline = logger.timeline_data()

        for job_key in our_timeline:

            # Get all the times in our job_key
            our_job_times = list()
            for _, timeline in our_timeline[job_key].items():
                our_job_times.extend(timeline)

            # Get all the times in their job_key
            their_job_times = list()
            for _, timeline in their_timeline[job_key].items():
                their_job_times.extend(timeline)

            job_points = {
                    "speedup": (max(their_job_times) - min(their_job_times)) / (max(our_job_times) - min(our_job_times)),
                    "turnaround": (max(their_job_times)) / (max(our_job_times)),
            }

            points[job_key] = job_points

        return points
