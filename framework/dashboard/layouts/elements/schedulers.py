from dash import Output, Input, State, dcc, html, callback, clientside_callback
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc
import json
import jsonpickle
from .updateschedulers import update_schedulers

stored_modules: dict[str, dict] = dict()

@callback(
        Output(component_id="schedulers-store", component_property="data"),
        State(component_id="schedulers-store", component_property="data"),
        Input(component_id="update-schedulers", component_property="n_intervals")
        )
def CB_update_schedulers(store_data, n_intervals):

    # If there is a new scheduling python module then re-issue the names of the
    # algorithms
    if update_schedulers(stored_modules):

        print(stored_modules)

        data = list()

        for mod_name, mod_dict in stored_modules.items():

            if mod_dict["viewable"]:

                if "Default" in mod_dict["classobj"].name:
                    data.insert(0, {
                        "name": mod_dict["classobj"].name,
                        "module": mod_name,
                        "selected": True,
                        "disabled": True
                    })
                
                elif "Random" in mod_dict["classobj"].name:

                    if data != [] and "Default" in data[0]["name"]:
                        data.insert(1, {
                            "name": mod_dict["classobj"].name,
                            "module": mod_name,
                            "selected": True,
                            "disabled": False
                        })

                    else:
                        data.insert(0, {
                            "name": mod_dict["classobj"].name,
                            "module": mod_name,
                            "selected": True,
                            "disabled": False
                        })

                else:
                    # Check if the scheduler existed in the previous data stored
                    # and if holds true check wether it was selected
                    index = None
                    for i, sched_dict in enumerate(store_data):
                        if mod_dict["classobj"].name in sched_dict:
                            index = i
                            break

                    data.append({
                        "name": mod_dict["classobj"].name,
                        "module": mod_name,
                        "selected": store_data[index] if index is not None else False,
                        "disabled": False
                    })

        return data

    else:

        # If nothing changed or there was a code change then only the backend
        # will deal with the changes
        raise PreventUpdate

clientside_callback(
        """
        function showMsg(data) {
            options = [];
            selected = [];
            for (i = 0; i < data.length; i++) {
                item = {
                    'label': data[i].name,
                    'value': i,
                    'disabled': data[i].disabled,
                };

                options.push(item);

                if (data[i].selected) {
                    selected.push(i)
                }
            }

            child = {
                'type': 'Checklist',
                'namespace': 'dash_bootstrap_components',
                'props': {
                    'id': 'selected-schedulers',
                    'options': options,
                    'value': selected,
                }
            };

            return child;

        }
        """,
        Output("schedulers-container", "children"),
        Input("schedulers-store", "data")
)


elem_schedulers = dbc.Container([

    dcc.Interval(id="update-schedulers", interval=10000),

    dcc.Store(id="schedulers-store", data=list(), storage_type="local"),

    dbc.Row([
        dbc.Col([dbc.CardImg(src="../../static/images/scheduler.svg")], width=2),
        dbc.Col([
            dbc.Row([html.H4("Schedulers")], class_name="py-1"),
            dbc.Row([html.P("""Select which scheduling algorithms will
                            participate in the simulation.""")]),
            ], width=10)
    ], align="center"),

    html.Hr(),

    dbc.Container(id="schedulers-container")
], style={"background-color": "lightgray",
          "height": "45vh",
          "border-radius": "10px"})
