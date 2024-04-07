import pymongo
from pymongo.server_api import ServerApi
import importlib

from dash import ClientsideFunction, Input, Output, State, clientside_callback, dcc, html
import dash_bootstrap_components as dbc

import os
import sys

# api
sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../../../../"
)))

# realsim
sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../../.."
)))

from realsim.generators.abstract import AbstractGenerator

# Get machines, suites per machine and workloads' names per machine per suite
client = pymongo.MongoClient(host="mongodb+srv://cslab:bQt5TU6zQsu_LZ@storehouse.om2d9c0.mongodb.net", 
                             server_api=ServerApi("1"))
# Get all the documents for workloads from the database
db = client["storehouse"]
collection  = db["loads"]
documents = [doc["_id"] for doc in collection.find({})]

# Close connection with the database for security reasons
client.close()

# Hold all the information about machines, suites, workloads and their
# hierarchical relationship
data: dict[str, dict[str, list]] = dict()

# Get all the machines names
machines = list(set([doc["machine"] for doc in documents]))
machines.sort()

# Initialize each machine and suite
for machine in machines:

    # Set an empty dictionary to populate with suites' names
    data[machine] = dict()

    # Get suites' names and sort them alphanumerically
    suites = list(set(
        [doc["suite"] for doc in documents if doc["machine"] == machine]
    ))
    suites.sort()

    # Initialize every suite with empty list for workloads' names

    # A suite that holds all the workloads
    data[machine]["All"] = []

    for suite in suites:
        data[machine][suite] = []

# Store all the workloads
for doc in documents:
    data[doc["machine"]]["All"].append(doc["load"])
    data[doc["machine"]][doc["suite"]].append(doc["load"])

# Sort all the workloads
for _, m_dict in data.items():
    for _, workloads in m_dict.items():
        workloads.sort(key=lambda name: (name.split(".")[0],
                                         int(name.split(".")[2]) ))


# Generators
gen_pack_dir = os.path.join(os.path.dirname(__file__), "../../../realsim/generators")
modules = [
        f"realsim.generators.{module}".replace(".py", "")
        for module in os.listdir(gen_pack_dir)
        if ".py" in module
]

for module in modules:
    importlib.import_module(module)

mapping = dict()
for gen_class in AbstractGenerator.__subclasses__():
    mapping[gen_class.name] = gen_class

gen_names_opt = dbc.Select(
        list(mapping.keys()), 
        list(mapping.keys())[0],
        id="generator-type"
)

elem_generator = dbc.Container([

    dcc.Store(id="generator-store", 
              data=data
              ),

    dbc.Row([
        dbc.Col([dbc.CardImg(src="../../assets/static/images/generator.svg")], width=1),
        dbc.Col([
            dbc.Row([html.P("Generator", className="h5"), html.P("""Select the 
            workloads from which the jobs will be created and the type of
            generator""")]),
            ])
    ], className="d-flex element-header sticky-top"), 

    # WORKLOADS ELEMENT ITEM
    dbc.Row([

        html.Span("Workloads", className="h6"),

        dbc.Col([ 
                 dbc.Button([
                     html.I(className="bi bi-database",
                            id="workloads-btn-class")
                ], id="workloads-btn"),
        ], width=1),

        dbc.Col([
            dbc.InputGroup([
                dbc.InputGroupText("Machine", style={"width": "30%"}),
                dbc.Select(list(data.keys()), 
                           list(data.keys())[0],
                           id="workloads-machines-select")
                ])
        ], width=6, id="workloads-machines-display", style={"display": "block"}),

        dbc.Col([
            dbc.InputGroup([
                dbc.InputGroupText("Suite", style={"width": "30%"}),
                dbc.Select(list(data[list(data.keys())[0]]), 
                           "All",
                           id="workloads-suites-select")
                ])
        ], width=5, id="workloads-suites-display", style={"display": "block"}),
        
        dbc.Col([
            dbc.Button("Upload directory", id="workloads-upload-directory",
                       style={"width": "100%"})
        ], width=11, id="workloads-upload-display", style={"display": "none"})


    ], class_name="element-item m-1 p-2"),

    # GENERATOR ELEMENT ITEM
    dbc.Row([

        html.Span("Generator", className="h6"),
        gen_names_opt 

        ], class_name="element-item m-1 p-2"),

    # GENERATOR OPTIONS ELEMENT ITEM
    dbc.Row([
        html.Span("Options", className="h6"),
        dbc.Spinner([

            dbc.Container(id="generator-options", style={"overflow": "scroll"})

                    ], color="primary")
        ], class_name="element-item m-1 p-2 d-flex align-self-strech")

    ], class_name="element d-flex align-items-strech")

# Clientside callback for workloads element-item management
clientside_callback(
        ClientsideFunction(
            namespace="clientside",
            function_name="workloads_events"
        ),
        Output("workloads-btn-class", "className"),
        Output("workloads-machines-select", "value"),
        Output("workloads-suites-select", "options"),
        Output("workloads-suites-select", "value"),
        Output("workloads-machines-display", "style"),
        Output("workloads-suites-display", "style"),
        Output("workloads-upload-display", "style"),

        Input("workloads-btn", "n_clicks"),
        Input("workloads-machines-select", "value"),
        State("workloads-suites-select", "options"),
        State("workloads-suites-select", "value"),
        State("generator-store", "data")
)

clientside_callback(
        ClientsideFunction(
            namespace="clientside",
            function_name="generator_options"
        ),
        Output("generator-options", "children"),

        Input("generator-type", "value"),
        Input("workloads-machines-select", "value"),
        Input("workloads-suites-select", "value"),
        State("generator-store", "data")
)
