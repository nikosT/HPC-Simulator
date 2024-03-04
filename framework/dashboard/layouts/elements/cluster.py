from dash import Input, Output, State, callback, ctx, dcc, html
import dash_bootstrap_components as dbc
import pymongo
from pymongo.server_api import ServerApi

# Automatic selection from machine names
client = pymongo.MongoClient(host="mongodb+srv://cslab:bQt5TU6zQsu_LZ@storehouse.om2d9c0.mongodb.net", 
                             server_api=ServerApi("1"))
db = client["storehouse"]
collection  = db["machines"]
machines = [doc["_id"] for doc in collection.find({})]
machines.sort()

client.close()

cluster_store = {"name": "", "nodes": 0, "ppn": 0}

elem_cluster = dbc.Container([

    dcc.Store(id="cluster-store", data=cluster_store, storage_type="session"),

    dbc.Row([
        dbc.Col([dbc.CardImg(src="../../static/images/cluster.svg")], width=2),
        dbc.Col([
            dbc.Row([html.H4("Cluster")], class_name="py-1"),
            dbc.Row([html.P("""Select by name or manually the specifications of
                            the cluster.""")]),
            ], width=10)
    ], align="center"),

    html.Hr(),

    dbc.Row([
        dbc.Button("To manual configuration", id="to-manual-btn")
        ], style={"display": "block"}, class_name="p-3", id="to-manual-display"),

    dbc.Row([
        dbc.Button("By name configuration", id="to-name-btn")
        ], style={"display": "none"}, class_name="p-3", id="to-name-display"),

    dbc.Container(id="cluster-options")

    ], style={"background-color": "lightgray", 
              "border-radius": "10px",
              "height": "45vh"})

@callback(
        Output(component_id="cluster-store", component_property="data"),
        Output(component_id="cluster-options", component_property="children"),
        Output(component_id="to-manual-display", component_property="style"),
        Output(component_id="to-name-display", component_property="style"),
        Input(component_id="to-manual-btn", component_property="n_clicks"),
        Input(component_id="to-name-btn", component_property="n_clicks"),
        Input(component_id="cluster-store", component_property="data"),
        Input(component_id="cluster-options", component_property="children")
        )
def cb_cluster(n1, n2, data, cluster_opts):

    visible = {"display": "block"}
    invisible = {"display": "none"}

    if ctx.triggered_id == "to-manual-btn":
        if data["nodes"] > 0:
            nodes = dbc.Col([ 
                             dbc.Input(type="number", 
                                       min=0, 
                                       value=data["nodes"],
                                       placeholder="number of nodes") 
                             ])
        else:
            nodes = dbc.Col([ 
                             dbc.Input(type="number", 
                                       min=0, 
                                       value=0,
                                       placeholder="number of nodes") 
                             ])

        if data["nodes"] > 0:
            ppn = dbc.Col([ 
                           dbc.Input(type="number", 
                                     min=0, 
                                     value=data["ppn"],
                                     placeholder="number of nodes") 
                           ])
        else:
            ppn = dbc.Col([ 
                           dbc.Input(type="number", 
                                     min=0,
                                     value=0,
                                     placeholder="number of nodes") 
                           ])

        manual_opts = dbc.Row([ nodes, ppn ], id="cluster-nodes-ppn")

        # Save machine name to data
        data["name"] = cluster_opts[0]["props"]["children"][0]["props"]["value"]

        return data, [manual_opts], invisible, visible

    else:
        if data["name"] != "":
            name_opts = dbc.Row([
                dbc.Select(machines, data["name"])
                ], id="cluster-name")
        else:
            name_opts = dbc.Row([
                dbc.Select(machines, machines[0])
                ], id="cluster-name")
            data["name"] = machines[0]

        # Save nodes and ppn to data
        if cluster_opts is not None:
            nodes_inp, ppn_inp = cluster_opts[0]["props"]["children"]

            nodes = nodes_inp["props"]["children"][0]["props"]["value"]
            if nodes is not None:
                data["nodes"] = int(nodes)

            ppn = ppn_inp["props"]["children"][0]["props"]["value"]
            if ppn is not None:
                data["ppn"] = int(ppn)

        return data, [name_opts], visible, invisible

