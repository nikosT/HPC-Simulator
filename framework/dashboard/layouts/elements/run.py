from dash import Output, Input, State, dcc, html, callback, clientside_callback, ClientsideFunction
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc
from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from numpy.random import seed, randint, exponential
from time import time_ns
import os
import sys

sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../../../../"
)))
from api.loader import LoadManager

sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../../../"
)))
from realsim.simulator import Simulation

from .generator import mapping
from .schedulers import stored_modules


elem_run = dbc.Container([

    dcc.Store(id="run-store"),

    dcc.Store(id="results-store"),

    # RUN AND VIEW BUTTONS
    dbc.Row([
        dbc.Col([dbc.CardImg(src="../../assets/static/images/run.svg")], width=1),
        dbc.Col([
            dbc.Row([html.H5("Run simulation"), 
                     html.P("""Select the number of experiments and if the
                            simulation will be dynamic""")])
            ], width=10)
    ], class_name="element-header sticky-top"),
    dbc.Row([
        dbc.Col([
            dbc.Button("Run simulation",
                       id="simulation-run-btn",
                       href='#',
                       class_name="d-flex align-items-center text-center",
                       style={"width": "100%", "borderRadius": "10px",
                              "height": "100%"}
                       )
            ], width=6),
        dbc.Col([
            dbc.Button("View results",
                       id="simulation-results-btn",
                       href='#',
                       class_name="btn-success d-flex align-items-center text-center",
                       style={"width": "100%", "borderRadius": "10px",
                              "height": "100%"}
                       )
            ], width=6),
    ], class_name="element-item m-1 p-2"),

    # 
    dbc.Row([

        html.Span("Run options", className="h6"),

        dbc.InputGroup([
            dbc.InputGroupText("Number of experiments", style={"width": "50%"}),
            dbc.Input(type="number", min=1, value=1, id="simulation-experiments")
        ]),

        dbc.InputGroup([
            dbc.InputGroupText("Simulation type", style={"width": "50%"}),
            dbc.Select(["Static", "Dynamic"], "Static", id='simulation-type')
        ]),
        dbc.InputGroup([
            dbc.InputGroupText("Distribution", style={"width": "50%"}),
            dbc.Select(["Constant", "Poisson", "Random"], "Constant",
                       id='simulation-distribution')
        ], id="simulation-distribution-display", style={"display": "none"}),

        dbc.Col([
            dbc.InputGroup([
                dbc.InputGroupText("Generator interval (sec)", style={"width": "50%"}),
                dbc.Input(value=0, type="number", min=0, 
                          id="generator-time")
            ]),
        ], id="generator-time-display", style={"display": "none"}),

    ], class_name="element-item m-1 p-2"),
    ], class_name="element d-flex align-items-strech")

clientside_callback(
        ClientsideFunction(
            namespace="clientside",
            function_name="run_options"
        ),

        Output("simulation-distribution-display", "style"),
        Output("generator-time-display", "style"),

        Input("simulation-type", "value"),

        prevent_initial_call=True
)

clientside_callback(
        ClientsideFunction(
            namespace="clientside",
            function_name="run_simulation"
        ),

        Output("run-store", "data"),

        Input("simulation-run-btn", "n_clicks"),

        # Generator args
        State("workloads-machines-select", "value"),
        State("workloads-suites-select", "value"),
        State("generator-type", "value"),
        State("generator-options", "children"),

        # Cluster args
        State("cluster-btn", "n_clicks"),
        State("cluster-store", "data"),
        State("cluster-machines", "value"),
        State("cluster-nodes", "value"),
        State("cluster-ppn", "value"),

        # Schedulers args
        State("schedulers-store", "data"),
        State("schedulers-container", "children"),

        # Run args
        State("simulation-experiments", "value"),
        State("simulation-type", "value"),
        State("simulation-distribution", "value"),
        State("generator-time", "value"),

        prevent_initial_call=True

)

def parallel_simulations(par_inp):

    # num, generator, gen_input, nodes, ppn, schedulers = par_inp
    generator_bundle, cluster_bundle, schedulers_bundle = par_inp

    # Unpack generator bundle
    lm, gen_class, gen_inp, sim_type, sim_dynamic_condition = generator_bundle

    # Create a generator of jobs
    generator = gen_class(lm)

    # Create a set of jobs based on generator's input
    jobs_set = generator.generate_jobs_set(gen_inp)

    # If simulation is dynamic then change the queued time of jobs
    if sim_type == "Dynamic":

        simulation_distribution = sim_dynamic_condition['distribution']
        generator_time = sim_dynamic_condition['generator-time']

        # Based on distribution
        if simulation_distribution == "Constant":
            for i, job in enumerate(jobs_set):
                    job.queued_time = i * generator_time
        elif simulation_distribution == "Random":
            seed(time_ns() % (2 ** 32))
            current_time = randint(low=0, high=generator_time, size=(1,))[0]
            for job in jobs_set:
                job.queued_time = current_time
                seed(time_ns() % (2 ** 32))
                current_time += randint(low=0, high=generator_time, size=(1,))[0]
        elif simulation_distribution == "Poisson":
            current_time = exponential(generator_time)
            for job in jobs_set:
                job.queued_time = current_time
                current_time += exponential(generator_time)
        else:
            pass

    # Unpack cluster bundle
    nodes, ppn = cluster_bundle

    # Setup simulation
    sim = Simulation(jobs_set, 
                     nodes, ppn, 
                     schedulers_bundle)
    sim.set_default("Default Scheduler")

    # Start simulation
    sim.run()

    # Get results from simulation
    res = sim.get_results()

    return res

@callback(
        Output("results-store", "data"),
        Output("results-modal", "is_open", True),
        Input("run-store", "data"),
        prevent_initial_call=True
)
def run_simulation(data):

    # Create load manager
    if data["workloads-suite"] == "All":
        lm = LoadManager(machine=data["workloads-machine"])
    else:
        lm = LoadManager(machine=data["workloads-machine"],
                         suite=data["workloads-suite"])
    lm.import_from_db(host="mongodb+srv://cslab:bQt5TU6zQsu_LZ@storehouse.om2d9c0.mongodb.net",
                      dbname="storehouse")

    # Setup generator bundle
    # TODO: change 'mapping' name
    gen_class = mapping[data["generator-type"]]
    gen_input = data["generator-input"]
    simulation_type = data["simulation-type"]
    simulation_dynamic_condition = data["simulation-dynamic-condition"]

    generator_bundle = (
            lm,
            gen_class,
            gen_input,
            simulation_type,
            simulation_dynamic_condition
    )

    # Setup cluster bundle
    cluster_bundle = (
            data["cluster-nodes"],
            data["cluster-ppn"]
    )

    # Setup schedulers bundle
    schedulers_bundle = []
    for name, hyperparams in data["schedulers"].items():
        sched_class = stored_modules[name]["classobj"]
        schedulers_bundle.append(
                (sched_class, hyperparams)
        )

    # Setup for parallel experiment execution
    num_of_experiments = int(data["simulation-experiments"])

    executor = ProcessPoolExecutor()
    futures = list()
    for _ in range(num_of_experiments):
        par_inp = (generator_bundle, cluster_bundle, schedulers_bundle)
        futures.append(
                executor.submit(parallel_simulations, par_inp)
        )

    print("<----- EXPERIMENTS SUBMITTED ----->")
    
    # Wait till all the experiments finish
    executor.shutdown(wait=True)
    
    print("<----- EXECUTOR FINISHED ----->")

    results = dict()
    for idx, future in enumerate(futures):
        results[f"Experiment {idx}"] = future.result()

    return results, True

clientside_callback(
        ClientsideFunction(
            namespace="clientside",
            function_name="create_results_menu"
            ),

        Output("results-nav", "children"),

        Input("results-store", "data"),

        prevent_initial_call=True

)
