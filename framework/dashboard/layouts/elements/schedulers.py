from inspect import isabstract
from dash import Output, Input, State, dcc, html, callback
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc
import importlib
import os
import sys
from glob import glob

import jsonpickle

# realsim
sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../../.."
)))

from realsim.scheduler.scheduler import Scheduler

def check_schedulers():

    schedulers_pack_dir = os.path.join(
            os.path.dirname(__file__), "../../../realsim/scheduler")

    modules = [
            f"realsim.scheduler.{module}".replace(".py", "").replace("/", ".")
            for module in glob("**/*.py", root_dir=schedulers_pack_dir,
                               recursive=True)
            if ".py" in module
    ]

    modules = list(filter(lambda mod: mod not in sys.modules, modules))

    return modules

def get_schedulers(modules):
    for module in modules:
        importlib.import_module(module)

def all_subclasses(cls):
    return set(cls.__subclasses__()).union(
            [s for c in cls.__subclasses__() for s in all_subclasses(c)]
    )

def update_schedulers():

    # Schedulers' storage
    schedulers_data = list()

    for scheduler_class in all_subclasses(Scheduler):
        if not isabstract(scheduler_class):
            schedulers_data.append(scheduler_class)

    schedulers_data.sort(key=lambda scheduler_class: scheduler_class.name)

    options = list()
    enabled_by_default = list()
    for i, scheduler_class in enumerate(schedulers_data):

        name = scheduler_class.name

        if "Default" in name:
            options.insert(0, {"label": name, "value": i, "disabled": True})
            enabled_by_default.append(i)
        elif "Random" in name:
            if options != []:
                if "Default" in options[0]["label"]:
                    options.insert(1, {"label": name, "value": i})
                else:
                    options.insert(0, {"label": name, "value": i})
            enabled_by_default.append(i)
        else:
            options.append({"label": name, "value": i})

    # Create list of checkboxes for schedulers
    schedulers_checkboxes = dbc.Checklist(options=options, 
                                          value=enabled_by_default,
                                          id="schedulers-checkboxes")

    schedulers_data = list(map(
        lambda scheduler_class: jsonpickle.encode( scheduler_class ),
        schedulers_data
    ))

    return schedulers_checkboxes, schedulers_data

modules = check_schedulers()
if modules != []:
    get_schedulers(modules)
schedulers_checkboxes, schedulers_data = update_schedulers()

@callback(
        Output(component_id="schedulers-store", component_property="data"),
        Output(component_id="schedulers-container", component_property="children"),
        Input(component_id="check-schedulers", component_property="n_intervals")
        )
def cb_schedulers(n_intervals):

    modules = check_schedulers()
    
    if modules == []:
        raise PreventUpdate

    get_schedulers(modules)
    schedulers_checkboxes, schedulers_data = update_schedulers()

    return schedulers_data, schedulers_checkboxes


elem_schedulers = dbc.Container([

    dcc.Interval(id="check-schedulers", interval=10000),

    dcc.Store(id="schedulers-store", data=schedulers_data),

    dbc.Row([
        dbc.Col([dbc.CardImg(src="../../static/images/scheduler.svg")], width=2),
        dbc.Col([
            dbc.Row([html.H4("Schedulers")], class_name="py-1"),
            dbc.Row([html.P("""Select which scheduling algorithms will
                            participate in the simulation.""")]),
            ], width=10)
    ], align="center"),

    html.Hr(),

    dbc.Container(schedulers_checkboxes, id="schedulers-container")
], style={"background-color": "lightgray",
          "height": "45vh",
          "border-radius": "10px"})
