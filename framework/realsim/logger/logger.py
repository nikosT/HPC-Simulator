import os
import sys

sys.path.append(os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "../../../"
        )
    ))

#if TYPE_CHECKING:
#    from realsim.cluster import ClusterV2
from realsim.jobs.jobs import EmptyJob, Job
from realsim.database import Database
import plotly.graph_objects as go
import plotly.express.colors as colors
from procset import ProcSet

class Logger(object):
    """
    Logs important events of the simulation into the memory
    for later use
    """

    def __init__(self, recording=True):
        # Controls if the events will be logged
        self.recording = recording
        #self.scale = colors.sequential.Turbo
        self.scale = colors.sequential.Turbo

    def assign_database(self, db: Database):
        self.database = db

    def assign_cluster(self, cluster):
        self.cluster = cluster

    def setup(self):

        # Cluster wide events
        self.cluster_events = dict()
        # self.cluster_events["checkpoints"] = set()
        # self.cluster_events["checkpoints"].add(0)
        self.cluster_events["checkpoints"] = [0]
        self.cluster_events["unused cores"] = list()
        self.cluster_events["deploying:spread"] = 0
        self.cluster_events["deploying:exec-colocation"] = 0
        self.cluster_events["deploying:wait-colocation"] = 0
        self.cluster_events["deploying:compact"] = 0
        self.cluster_events["deploying:success"] = 0
        self.cluster_events["deploying:failed"] = 0
        self.cluster_events["finished jobs"] = [0]

        # Events #
        # Job events
        self.job_events: dict[str, dict] = dict()

        # Init job events
        for job in self.database.preloaded_queue:
            # Job events
            jevts = {
                    "trace": [], # [co-job, start time, end time]
                    "speedups": [], # [sp1, sp2, ..]
                    "cores": dict(), # {cojob1: cores1, cojob2: cores2, ..}
                    "assigned procs": ProcSet(),
                    "remaining time": [],
                    "start time": 0,
                    "finish time": 0,
                    "submit time": 0,
                    "waiting time": 0,
                    "wall time": job.wall_time,
                    "num of processes": job.num_of_processes
            }
            self.job_events[f"{job.job_id}:{job.job_name}"] = jevts

    def evt_jobs_executing(self):
        """Record events of jobs that are still executing such as:

            - trace: point in time running alone/spread/with another job

            - speedup: the speedup of a job at the specific point in time

            - cores: binded cores at the specific point in time running
              alone/spread/with another job

            - remaining time: the remaining time of a job at the specific point
              in time with the specific speedup
        """

        if len(self.cluster_events["unused cores"]) == len(self.cluster_events["checkpoints"]) - 1:
            self.cluster_events["unused cores"].append(len(self.cluster.total_procs))
        else:
            self.cluster_events["unused cores"][-1] = len(self.cluster.total_procs)

        for xunit in self.cluster.execution_list:

            if type(xunit[-1]) == EmptyJob:
                self.cluster_events["unused cores"][-1] += len(xunit[-1].assigned_cores)

            if len(xunit) == 1 and type(xunit[0]) != EmptyJob:

                job_key = f"{xunit[0].job_id}:{xunit[0].job_name}"

                # Get the starting time of job for each checkpoint
                self.job_events[job_key]["trace"].append(
                        ["compact", self.cluster.makespan, None]
                )

                self.job_events[job_key]["speedups"].append(xunit[0].sim_speedup)

                # Get compact cores usage
                self.job_events[job_key]["cores"].update({
                    "compact": len(xunit[0].assigned_cores)
                })

                self.job_events[job_key]["remaining time"].append(
                        xunit[0].remaining_time
                )

            elif len(xunit) > 1:
                # Find if every job has finished
                empty = True
                for job in xunit:
                    if type(job) != EmptyJob:
                        empty = False
                        break
                
                if empty:
                    continue

                head_key = f"{xunit[0].job_id}:{xunit[0].job_name}"

                # Get tail key
                tail_key = "spread"
                tail = [job for job in xunit[1:] if type(job) != EmptyJob ]

                if tail != []:

                    tail_key = "|".join([
                        f"{job.job_id}:{job.job_name}" for job in tail
                        if type(job) != EmptyJob
                    ])

                    for job in tail:

                        job_key = f"{job.job_id}:{job.job_name}"

                        self.job_events[job_key]["trace"].append(
                                [head_key, self.cluster.makespan, None]
                        )

                        self.job_events[job_key]["speedups"].append(
                                job.sim_speedup
                        )

                        self.job_events[job_key]["cores"].update({
                            head_key: len(job.assigned_cores)
                        })

                        self.job_events[job_key]["remaining time"].append(
                                job.remaining_time
                        )

                self.job_events[head_key]["trace"].append(
                        [tail_key, self.cluster.makespan, None]
                )
                
                self.job_events[head_key]["speedups"].append(xunit[0].sim_speedup)

                self.job_events[head_key]["cores"].update({
                    tail_key: len(xunit[0].assigned_cores)
                })

                self.job_events[head_key]["remaining time"].append(
                        xunit[0].remaining_time
                )

            else:
                pass

    def evt_job_finishes(self, job: Job):
        """Record time when job finished
        """

        job_key = f"{job.job_id}:{job.job_name}"

        # Set ending time in last trace
        self.job_events[job_key]["trace"][-1][-1] = self.cluster.makespan

        # Set the assigned processors for the job
        self.job_events[job_key]["assigned procs"] = job.assigned_cores


        # Record the arrival and waiting time
        self.job_events[job_key]["start time"] = job.start_time
        self.job_events[job_key]["finish time"] = self.cluster.makespan
        self.job_events[job_key]["submit time"] = job.submit_time
        self.job_events[job_key]["waiting time"] = job.waiting_time

        # Write down the total number of finished job and unused cores at the 
        # checkpoint
        self.cluster_events["finished jobs"][-1] += 1


    def get_gantt_representation(self):

        # Create the color palette for each job
        num_of_jobs = len(self.job_events.keys())
        jcolors = colors.sample_colorscale(self.scale, [n/(num_of_jobs - 1) for n in range(num_of_jobs)])

        # Create data for figure
        fig_data = list()

        for idx, [key, jevt] in enumerate(self.job_events.items()):

            for interval in jevt["assigned procs"].intervals():
                x_min = jevt["start time"]
                x_max = jevt["finish time"]
                y_min = interval.inf
                y_max = interval.sup

                xs = [x_min, x_max, x_max, x_min, x_min]
                ys = [y_min, y_min, y_max, y_max, y_min]

                fig_data.append(go.Scatter(
                    x=xs,
                    y=ys,
                    mode="lines",
                    legendgroup=key,
                    line=dict(width=0.1, color="black"),
                    fill="toself",
                    fillcolor=jcolors[idx],
                    showlegend=False,
                    name=f"<b>{key}</b><br>"+
                    f"submit time = {jevt['submit time']:.2f} s<br>"+
                    f"start time = {jevt['start time']:.2f} s<br>"+
                    f"finish time = {jevt['finish time']:.2f} s<br>"+
                    f"waiting time = {jevt['waiting time']:.2f} s<br>"+
                    f"processors = {len(jevt['assigned procs'])}",
                ))

        fig = go.Figure(data=fig_data)
        fig.update_layout(
                title=f"<b>{self.cluster.scheduler.name}</b><br>Gantt Plot",
                title_x=0.5,
                yaxis=dict(
                    title="<b>Cores</b>",
                    range=[0, self.cluster.total_cores],
                    tickmode="array",
                    tickvals=[self.cluster.total_cores],
                ),
                xaxis=dict(
                    title="<b>Time</b>"
                ),
                template="seaborn"
        )
        return fig.to_json()

    def get_history_trace(self):
        """Get the execution history for each job that was deployed to the
        cluster

        {
            job_key_1: {
                cojob_key_1_1: [starting time, end time],
                cojob_key_1_2: [starting time 2, end time 2],
                .....
                ....
                cojob_key_1_n: [starting time n, end time n],
            },

            job_key_2: {
                cojob_key_2_1: [starting time, end time],
                cojob_key_2_2: [starting time 2, end time 2],
                .....
                ....
                cojob_key_2_k: [starting time k, end time k],
            },

            ......
            ....



        }
        """

        # Historical data for each job
        # job_key: {cojob_key: [start time, end time]}, {cojob_key2: [st2, et2]}
        historical_data: dict[str, dict] = dict()

        # CONSTANTS
        CJ_KEY = 0 # co-job key
        START = 1 # starting time
        END = 2 # end time

        for job_key, job_event in self.job_events.items():

            traces = job_event["trace"]

            # Some runtimes lists will be empty
            # because we didn't want to save information twice
            if traces == []:
                continue

            # Initialize a dictionary for each non empty job key
            historical_data[job_key] = dict()

            # Get starting time of job and the first co-scheduled job key
            first_trace = traces[0]
            current_cojob_key = first_trace[CJ_KEY]
            current_starting_time = first_trace[START]

            # Initialize historical data
            historical_data[job_key].update({

                current_cojob_key: 
                [
                    [current_starting_time, first_trace[END]]
                ]

            })

            for trace in traces:

                # If the co-scheduled job(s) changed then record the history
                # with the previous job and move on to the next jobs
                if current_cojob_key != trace[CJ_KEY]:

                    # Set end time for the previouse co-job key
                    historical_data[job_key][current_cojob_key][-1][-1] = trace[START]
                    
                    # Set the current cojob key and starting time
                    current_cojob_key = trace[CJ_KEY]
                    current_starting_time = trace[START]

                    # If the newly found co-job key already exists then create a
                    # new list
                    if current_cojob_key in historical_data[job_key]:
                        historical_data[job_key][current_cojob_key].append([
                            current_starting_time, trace[END]
                        ])
                    # Else create a new record for the new co-job key
                    else:
                        historical_data[job_key].update({

                            current_cojob_key: 
                            [
                                [current_starting_time, first_trace[END]]
                            ]

                        })


            # Get the end time from last trace
            historical_data[job_key][current_cojob_key][-1][-1] = traces[-1][END]

        return historical_data

    def get_resource_usage(self):

        # Get all the checkpoints of the simulation
        checks = list(sorted(
                self.cluster_events["checkpoints"]
        ))

        # Calculate the intervals between the checkpoints
        intervals = [b - a for a, b in list(zip(
            checks[:len(checks)], checks[1:]
        ))]

        # Get the history trace of the simulation
        history_trace = self.get_history_trace()

        # Create the color palette for each job
        num_of_jobs = len(history_trace.keys())
        jcolors = colors.sample_colorscale(self.scale, [n/(num_of_jobs - 1) for n in range(num_of_jobs)])

        traces = list()
        for idx, [job_key, job_history] in enumerate(history_trace.items()):

            for cojob_key, list_times in job_history.items():

                for times in list_times:

                    job_cores = self.job_events[job_key]["cores"][cojob_key]

                    # Calculate unused cores from an xunit
                    unused_cores = 0
                    if cojob_key != "spread" and cojob_key != "compact":
                        
                        lesser_cores = sum([
                            self.job_events[cj_key]["cores"][job_key]
                            for cj_key in cojob_key.split("|")
                            if job_key in self.job_events[cj_key]["cores"].keys()
                        ])

                        # We only show unused cores for the big job so as to not
                        # duplicate their value in the graph
                        if lesser_cores > 0 and lesser_cores < job_cores:
                            unused_cores = job_cores - lesser_cores

                    elif cojob_key == "spread":
                        unused_cores = job_cores

                    # The position of each box on the x-axis (time axis)
                    xs = [
                        checks[i] + intervals[i] / 2
                        # We don't care about the last checkpoint because all jobs
                        # have finished by that time
                        for i, time in enumerate(checks[:len(checks)])
                        if time >= times[0] and time < times[1]
                    ]

                    # The width of each box
                    ws = [
                        intervals[i]
                        for i, time in enumerate(checks[:len(checks)])
                        if time >= times[0] and time < times[1]
                    ]

                    # The height of each box
                    ys = [job_cores] * len(xs)

                    # The label on top of the box
                    if cojob_key == "compact":
                        text = f"<b>{job_key}<br>[{cojob_key}]</b><br>nodes = {int(job_cores / self.cluster.cores_per_node)}"
                    else:
                        text = f"<b>{job_key}<br>[{cojob_key}]</b><br>nodes = {int(2 * job_cores / self.cluster.cores_per_node)}"

                    # Add the box to the plot
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

                    # If there are unused cores inside the xunit then show them
                    if unused_cores > 0:

                        traces.append(
                                go.Bar(
                                    x=xs,
                                    y=[unused_cores] * len(xs),
                                    width=ws,
                                    name=f"{job_key.split(':')[0]}:unused",
                                    text="",
                                    insidetextanchor="middle",
                                    marker_line=dict(width=2, color=jcolors[idx]),
                                    marker=dict(
                                        color=jcolors[idx],
                                        pattern=dict(
                                            fillmode="replace",
                                            shape="x",
                                            size=6,
                                            solidity=0.7
                                        )
                                    ),
                                )
                        )

        # Create figure with the box resource usage
        fig = go.Figure(data=traces)

        # Draw vertical lines for each checkpoint
        for check in checks:
            fig.add_vline(x=check, line_width=1, line_dash="dot",)

        # Change the layout of the plot
        fig.update_layout(
                title=f"<b>{self.cluster.scheduler.name}</b><br>Resources usage",
                title_x=0.5,
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

        return fig.to_json()

    def get_jobs_utilization(self, logger):
        """Get different utilization metrics for each job in comparison to
        another (common use: default scheduling) logger
        """

        if not isinstance(logger, Logger):
            raise Exception("Provide a Logger instance")

        # Boxplot points
        points = dict()

        # Get the time spent on a job in both loggers
        our_history = self.get_history_trace()
        their_history = logger.get_history_trace()

        for job_key in our_history:

            # Get all the times in our job_key
            our_job_times = list()
            for _, trace in our_history[job_key].items():
                for times in trace:
                    our_job_times.extend(times)

            # Get all the times in their job_key
            their_job_times = list()
            for _, trace in their_history[job_key].items():
                for times in trace:
                    their_job_times.extend(times)

            # Utilization numbers
            job_points = {
                    "speedup": (max(their_job_times) - min(their_job_times)) / (max(our_job_times) - min(our_job_times)),
                    "turnaround": (max(their_job_times) - logger.job_events[job_key]["submit time"]) / (max(our_job_times) - self.job_events[job_key]["submit time"]),
                    "waiting": logger.job_events[job_key]["waiting time"] - self.job_events[job_key]["waiting time"]
            }

            points[job_key] = job_points

        return points

    def get_waiting_queue_graph(self):
        num_of_jobs: list[int] = list()
        for check in sorted(list(self.cluster_events["checkpoints"])):
            jobs_in_check = 0
            for _, jevt in self.job_events.items():
                if jevt["submit time"] <= check and jevt["start time"] > check:
                    jobs_in_check += 1
            num_of_jobs.append(jobs_in_check)

        return (
                sorted(list(self.cluster_events["checkpoints"])),
                num_of_jobs
        )

    def get_jobs_throughput(self):
        return (
                sorted(list(self.cluster_events["checkpoints"])),
                self.cluster_events["finished jobs"]
        )

    def get_jobs_throughput_derivative(self):
        checkpoints = self.cluster_events["checkpoints"]
        finished_jobs = self.cluster_events["finished jobs"]
        derivatives = [
                (finished_jobs[i] - finished_jobs[i-1]) / (checkpoints[i] - checkpoints[i-1])
                for i in range(1, len(checkpoints))
        ]

    def get_unused_cores_graph(self):
        self.cluster_events["unused cores"].append(self.cluster.total_cores)
        return (
                sorted(list(self.cluster_events["checkpoints"])),
                self.cluster_events["unused cores"]
        )

    def get_workload(self):
        """Return 1-5 and 9 fields frm the Standart Workload Format
        """

        header = "Job Number,"
        header += "Submit Time,Wait Time,Run Time," # Actual times
        header += "Number of Allocated Processors,Average CPU Time Used,Used Memory," # Used resources
        header += "Requested Number of Processors,Requested Time,Requested Memory," # Requested resources
        header += "Status,User ID,Group ID,Executable Number," # Assign job_name
        header += "Queue Number,Partition Number,Preceding Job Number,Think Time from Preceding Job\n" # Irrelevant for us

        workload = ""
        for jevt_id, jevt in self.job_events.items():
            job_id, job_name = jevt_id.split(':')
            workload += f"{job_id},"
            workload += f"{jevt['submit time']},{jevt['waiting time']},{jevt['finish time']-jevt['start time']},"
            workload += f"{len(jevt['assigned procs'])},,,"
            workload += f"{jevt['num of processes']},{jevt['wall time']},,"
            workload += f"1,,,{job_name},"
            workload += f",,,\n"

        return header + workload
