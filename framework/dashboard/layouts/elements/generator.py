from typing import Optional
import pymongo
from pymongo.server_api import ServerApi
import importlib
import jsonpickle
import inspect

from dash import Input, Output, State, callback, dcc, html, ctx
from dash.exceptions import PreventUpdate
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

from api.loader.manager import LoadManager
from realsim.generators.abstract import AbstractGenerator

# Automatic selection from machine names
client = pymongo.MongoClient(host="mongodb+srv://cslab:bQt5TU6zQsu_LZ@storehouse.om2d9c0.mongodb.net", 
                             server_api=ServerApi("1"))
db = client["storehouse"]
collection  = db["loads"]
machines = list(set([
    doc["_id"]["machine"] for doc in collection.find({})
]))
machines.sort()

input_from_db = dbc.Select(machines, machines[0], id="db-input")
input_from_folder = dbc.Button("Upload directory", id="folder-input", 
                               style={"width": "100%"})

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

    dcc.Store(id="generator-store", storage_type="session"),

    dbc.Row([
        dbc.Col([dbc.CardImg(src="../../static/images/generator.svg")], width=2),
        dbc.Col([
            dbc.Row([html.H4("Generator")], class_name="py-1"),
            dbc.Row([html.P("""Select the loads from which the jobs will be
                            created and the type of generator.""")]),
            ], width=10)
    ], align="center"),

    html.Hr(),
    
    dbc.Row([ dbc.Label("Loads") ], class_name="py-1"),
    dbc.Row([

        dbc.Col([ 
                 dbc.Button([html.I(className="bi bi-database")],
                            id="db-input-btn"),
        ], style={"display": "block"}, id="db-input-display"),

        dbc.Col([ 
                 dbc.Button([html.I(className="bi bi-upload")],
                            id="folder-input-btn"),
        ], style={"display": "none"}, id="folder-input-display"),

        dbc.Col([ input_from_db ], width=10, id="input-type")

    ], class_name="d-flex"),

    dbc.Row([ dbc.Label("Generator") ], class_name="py-1"),
    dbc.Row([ gen_names_opt ], class_name="px-3"),

    dbc.Row([ dbc.Label("Options") ]),
    dbc.Row([
        dbc.Spinner([
            dbc.Container(id="generator-options", 
                          style={"overflow": "scroll", 
                                 "height": "10vh"})
                    ], color="primary")
        ], class_name="pb-2")

    ], style={"background-color": "lightgray", "border-radius": "10px"})

@callback(
        Output(component_id="input-type", component_property="children"),
        Output(component_id="db-input-display", component_property="style"),
        Output(component_id="folder-input-display", component_property="style"),
        Input(component_id="db-input-btn", component_property="n_clicks"),
        Input(component_id="folder-input-btn", component_property="n_clicks"),
        )
def cb_generator(n1, n2):

    visible = {"display": "block"}
    invisible = {"display": "none"}

    if ctx.triggered_id == "db-input-btn":
        return [input_from_folder], invisible, visible
    else:
        return [input_from_db], visible, invisible

def get_options(generator_type, lm : Optional[LoadManager] = None):

    if generator_type == "Random Generator":
        return [ dbc.InputGroup([
            dbc.InputGroupText("Number of jobs", style={"width": "50%"}),
            dbc.Input(type="number", min=1, value=1, )
            ]) ]
    elif generator_type == "Dictionary Generator":
        children = list()
        for name in sorted(lm.loads):
            children.append(dbc.InputGroup([
                dbc.InputGroupText(f"{name}", style={"width": "50%"}),
                dbc.Input(type="number", value=0, min=0)
            ]))

        return children
    elif generator_type == "List Generator":
        return [ dcc.Upload([
            dbc.Button("Upload file with list of jobs", id="list-gen-input",
                       style={"width": "100%"})
            ], id="upload-list-gen-input") ]
    else:
        return []

@callback(
        Output(component_id="generator-store", component_property="data"),
        Output(component_id="generator-options", component_property="children"),
        Input(component_id="generator-store", component_property="data"),
        Input(component_id="db-input", component_property="value"),
        Input(component_id="generator-type", component_property="value"),
        )
def storage_db(data, db_input, gen_type):

    if data is None:
        new_data = dict()
    else:
        new_data = data

    if ctx.triggered_id == "db-input":
        lm = LoadManager(db_input, suite="NAS") # Add suite selection
        lm.import_from_db(host="mongodb+srv://cslab:bQt5TU6zQsu_LZ@storehouse.om2d9c0.mongodb.net", 
                          dbname="storehouse")
        new_data["load-manager"] = jsonpickle.encode( lm )
        new_data["generator"] = jsonpickle.encode( mapping[gen_type](lm) )
    elif ctx.triggered_id == "generator-type":
        lm = jsonpickle.decode( data["load-manager"] )
        new_data["generator"] = jsonpickle.encode( mapping[gen_type](lm) )
    else:
        raise PreventUpdate

    if gen_type == "Dictionary Generator":
        return new_data, get_options(gen_type, jsonpickle.decode( new_data["load-manager"] ))
    else:
        return new_data, get_options(gen_type)

