from dash import Output, Input, State, dcc, html, callback, clientside_callback, ClientsideFunction
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc
from concurrent.futures import ProcessPoolExecutor
import os
import sys

sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../../../../"
)))
from api.loader import LoadManager

sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../../../"
)))
from realsim.cluster.exhaustive import ClusterExhaustive
from realsim.simulator import Simulation

from .generator import mapping
from .schedulers import stored_modules


elem_run = dbc.Container([

    dcc.Store(id="results-store"),

    dcc.Store(id="run-store"),

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
                       style={"width": "100%", "borderRadius": "10px",
                              "height": "100%"}
                       )
            ], width=6),
        dbc.Col([
            dbc.Button("View results",
                       id="simulation-results-btn",
                       class_name="btn-success",
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
            dbc.InputGroupText("Stop condition", style={"width": "50%"}),
            dbc.Select(["Time", "Number of jobs"], "Time",
                       id='simulation-stop-conditions')
        ], id="simulation-stop-conditions-display", style={"display": "none"}),

        dbc.Col([
            dbc.InputGroup([
                dbc.InputGroupText("Stop time (sec)"),
                dbc.Input(value=0, type="number", min=0, 
                          id="simulation-time-condition")
            ]),
            dbc.InputGroup([
                dbc.InputGroupText("Generator pulse (sec)"),
                dbc.Input(value=0, type="number", min=0, 
                          id="generator-time-condition")
            ]),
        ], id="simulation-time-condition-display", style={"display": "none"}),

        dbc.InputGroup([
            dbc.InputGroupText("Jobs to finish", style={"width": "50%"}),
            dbc.Input(value=0, type="number", min=0, 
                      id="simulation-jobs-condition")
        ], id="simulation-jobs-condition-display", style={"display": "none"}),

    ], class_name="element-item m-1 p-2"),
    ], class_name="element d-flex align-items-strech")

clientside_callback(
        ClientsideFunction(
            namespace="clientside",
            function_name="run_options"
        ),

        Output("simulation-stop-conditions-display", "style"),
        Output("simulation-time-condition-display", "style"),
        Output("simulation-jobs-condition-display", "style"),

        Input("simulation-type", "value"),
        Input("simulation-stop-conditions", "value"),

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
        State("simulation-stop-conditions", "value"),
        State("simulation-time-condition", "value"),
        State("generator-time-condition", "value"),
        State("simulation-jobs-condition", "value"),

        prevent_initial_call=True

)

def parallel_simulations(par_inp):

    print("INTO parallel_simulations")

    # num, generator, gen_input, nodes, ppn, schedulers = par_inp
    num, generator_bundle, cluster_bundle, schedulers_bundle = par_inp

    # Unpack generator bundle
    lm, gen_class, gen_input, simulation_type, simulation_stop_condition = generator_bundle
    generator = gen_class(lm)

    # Unpack cluster bundle
    nodes, ppn = cluster_bundle

    # Setup simulation
    sim = Simulation(generator, gen_input, 
                     simulation_type, simulation_stop_condition, 
                     nodes, ppn, 
                     schedulers_bundle)
    sim.set_default("Default Scheduler")

    print("BEFORE sim.run()")
    
    sim.run()

    # Draw the figures
    # figures = sim.plot()
    sim.plot()

@callback(
        Output("results-store", "data"),
        Input("run-store", "data"),
        prevent_initial_call=True
)
def run_simulation(data):

    print(data)

    # Create load manager
    if data["workloads-suite"] == "All":
        lm = LoadManager(machine=data["workloads-machine"])
    else:
        lm = LoadManager(machine=data["workloads-machine"],
                         suite=data["workloads-suite"])
    lm.import_from_db(host="mongodb+srv://cslab:bQt5TU6zQsu_LZ@storehouse.om2d9c0.mongodb.net",
                      dbname="storehouse")

    # Instantiate a generator
    # TODO: change 'mapping' name
    gen_class = mapping[data["generator-type"]]
    gen_input = data["generator-input"]

    # Setup generator bundle
    simulation_type = data["simulation-type"]
    simulation_stop_condition = data["simulation-stop-condition"]
    generator_bundle = (
            lm,
            gen_class,
            gen_input,
            simulation_type,
            simulation_stop_condition
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

    executor = ProcessPoolExecutor(max_workers=os.cpu_count())
    futures = list()
    for idx in range(num_of_experiments):
        par_inp = (idx, generator_bundle, cluster_bundle, schedulers_bundle)
        print(par_inp)
        futures.append(
                executor.submit(parallel_simulations, par_inp)
        )
    print("EXPERIMENTS SUBMITTED")
    # Wait till all the experiments finish
    executor.shutdown(wait=True)
    
    print("EXECUTOR FINISHED")

    return data
