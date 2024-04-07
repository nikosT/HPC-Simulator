import cProfile
import plotly.graph_objects as go
import os
import sys

sys.path.append(os.path.abspath(
    os.path.dirname(__file__)
))
sys.path.append(os.path.abspath(
    os.path.join(
        os.path.dirname(__file__),
        "../"
    )
))

from api.loader import LoadManager
from realsim.generators.random import RandomGenerator
from realsim.generators.keysdict import KeysDictGenerator

from realsim.cluster.shallow import ClusterShallow
from realsim.scheduler.compact import CompactScheduler
from realsim.logger.logger import Logger

from realsim.cluster.exhaustive import ClusterExhaustive
from realsim.scheduler.balancerFullOn import BalancerFullOn
from realsim.scheduler.coschedulers.ranks.balancing import BalancingRanksCoscheduler

from realsim.scheduler.random import RandomScheduler

# Create a load manager and import loads from database
lm = LoadManager("aris.compute", "NAS")
lm.import_from_db(username="admin", password="admin", dbname="storehouse")
try:
    lm.loads.pop("613.soma_s.1024")
except:
    pass
try:
    lm.loads.pop("cg.E.2048")
except:
    pass

lm.loads.pop("bt.E.2025")
lm.loads.pop("sp.E.2025")
# lm.loads.pop("cg.E.2048")
lm.loads.pop("ft.E.2048")
lm.loads.pop("lu.E.2048")

# Create random job generator from load manager
gen = RandomGenerator(lm)
kdict_gen = KeysDictGenerator(lm)

speedups = list()
boxpoints = list()
turnaround = list()
exps = int(input("How many experiments? "))
for _ in range(exps):

    # Generate a job set
    jobs_set = gen.generate_jobs_set(200)
    # jobs_set = kshuffled_gen.generate_jobs_set({
    #     "bt.D.256": 30,
    #     "lu.E.512": 50
    # })
    # jobs_set.sort(key=lambda job: job.num_of_processes, reverse=True)

    # COMPACT
    cluster = ClusterExhaustive(426, 20)
    cluster.deploy_to_waiting_queue(jobs_set)
    compact = CompactScheduler()
    logger = Logger()

    cluster.assign_scheduler(compact)
    compact.assign_cluster(cluster)
    cluster.assign_logger(logger)
    compact.assign_logger(logger)

    cluster.run()

    fig = logger.plot_resources()
    fig.show()

    # FULLON
    cluster_fullon = ClusterExhaustive(426, 20)
    cluster_fullon.deploy_to_waiting_queue(jobs_set)

    balancer_fullon = BalancingRanksCoscheduler()
    #balancer_fullon = BalancerFullOn()
    #balancer_fullon = RandomScheduler()

    logger_fullon = Logger()

    cluster_fullon.assign_scheduler(balancer_fullon)
    balancer_fullon.assign_cluster(cluster_fullon)

    cluster_fullon.assign_logger(logger_fullon)
    balancer_fullon.assign_logger(logger)

    cluster_fullon.run()

    # print( logger_fullon.job_events )

    fig = logger_fullon.plot_resources()
    fig.show()

    boxpoints.append(
            logger_fullon.jobs_speedup_boxpoints(logger)
    )

    turnaround.append(
            logger_fullon.jobs_boxpoints(logger)
    )

    speedup = cluster.makespan / cluster_fullon.makespan

    speedups.append(speedup)

    # Calculate job throughput
    compact_jobs = 0
    coschedule_jobs = 0
    compact_90 = cluster.makespan * 0.8


fig = go.Figure()

for i, points in enumerate(boxpoints):
    fig.add_trace(
            go.Box(
                y=points,
                x=[i]*len(points),
                name=f"Exp{i}",
                x0=f"Exp{i}",
                boxpoints="all",
                showlegend=False,
                boxmean="sd"
            )
    )

fig.add_trace(
        go.Scatter(x=list(range(len(speedups))), 
                   y=speedups, mode="lines+markers",
                   marker=dict(color="black"), name="Experiments' Speedup"
        )
)

fig.add_hline(y=1, line_color="black", line_dash="dot")

fig.update_layout(
        title=f"<b>Jobs' speedups</b>",
        title_x=0.5,
        xaxis=dict(
            title="<b>Experiments</b>",
            tickmode="array",
            tickvals=list( range(exps) ),
            ticktext=[f"Exp{i}" for i in range(exps)]
        ),
        yaxis=dict(title="<b>Speedup</b>")
)

fig.show()


fig = go.Figure()

for i, points in enumerate(turnaround):
    names = list()
    t_values = list()
    s_values = list()
    for name, value in points.items():
        names.append(name)
        s_values.append(value["speedup"])
        t_values.append(value["turnaround"])

    fig.add_trace(
            go.Box(
                y=t_values,
                x=[i]*len(points),
                name="turnaround",
                x0=f"Exp{i}",
                boxpoints="all",
                boxmean="sd",
                text=names,
                marker_color="blue",
                showlegend=False
            )
    )
    fig.add_trace(
            go.Box(
                y=s_values,
                x=[i]*len(points),
                name="speedup",
                x0=f"Exp{i}",
                boxpoints="all",
                boxmean="sd",
                text=names,
                marker_color="red",
                showlegend=False
            )
    )

fig.add_trace(
        go.Scatter(x=list(range(len(speedups))), 
                   y=speedups, mode="lines+markers",
                   marker=dict(color="black"), name="Experiments' Speedup"
        )
)

fig.add_hline(y=1, line_color="black", line_dash="dot")

fig.update_layout(
        title=f"<b>Jobs' turnaround</b>",
        title_x=0.5,
        xaxis=dict(
            title="<b>Experiments</b>",
            tickmode="array",
            tickvals=list( range(exps) ),
            ticktext=[f"Exp{i}" for i in range(exps)]
        ),
        yaxis=dict(title="<b>Speedup</b>")
)

fig.show()

#app = Dash(__name__,)
#app.layout = html.Div([
#    dcc.Graph(figure=fig, style={"height": "100vh"})
#    ])
#
#if __name__ == "__main__":
#    app.run(debug=False)
