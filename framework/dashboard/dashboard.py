from uuid import uuid4
from dash import Dash, Output, dcc, html, callback
import dash_bootstrap_components as dbc

from layouts.main import main_layout

import os
import sys

# Import api
sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../../"
)))

# Import realsim
sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../"
)))

app = Dash(__name__,
           compress=True,
           meta_tags=[{"name": "viewport", "content": "width=device-width, initial-scale=1"}],
           external_stylesheets=[dbc.themes.BOOTSTRAP, dbc.icons.BOOTSTRAP])

# Dynamic layout
app.config.suppress_callback_exceptions = True

app.layout = dbc.Container([
        dcc.Store(id="app-store", 
                  storage_type="session", 
                  data=dict(sid=str(uuid4()))
                  ),
        main_layout
        ], fluid=True, class_name="mh-100", id="layout")

if __name__ == "__main__":
    # Start application
    app.run(host="0.0.0.0", debug=True)

