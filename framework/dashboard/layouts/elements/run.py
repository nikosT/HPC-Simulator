from dash import Output, Input, State, dcc, html, callback, clientside_callback, ClientsideFunction
from dash.exceptions import PreventUpdate
import dash_bootstrap_components as dbc


elem_run = dbc.Container([
    dbc.Row([
        dbc.Col([dbc.CardImg(src="../../assets/static/images/run.svg")], width=1),
        dbc.Col([
            dbc.Row([html.H5("Run simulation"), 
                     html.P("""Select the number of experiments and if the
                            simulation will be dynamic""")])
            ], width=10)
    ], class_name="element-header sticky-top"),
    dbc.Row([
        dbc.InputGroup([
            dbc.InputGroupText("Number of experiments"),
            dbc.Input(type="number", min=1, value=1, id="num-of-experiments")
            ]),
        ], class_name="py-1"),
    dbc.Row([
        dbc.Col([
            dbc.Button("Run simulation",
                       id="run-simulation",
                       style={"width": "100%", "border-radius": "10px"}
                       )
            ], width=6),
        dbc.Col([
            dbc.Button("View results",
                       id="view-results",
                       class_name="btn-success",
                       style={"width": "100%", "border-radius": "10px"}
                       )
            ], width=6),
        ], class_name="py-1")
    ], class_name="element")
