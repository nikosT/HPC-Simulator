from dash import ClientsideFunction, Input, Output, State, callback, clientside_callback, ctx, dcc, html
import dash_bootstrap_components as dbc
import pymongo
from pymongo.server_api import ServerApi

# Automatic selection from machine names
client = pymongo.MongoClient(host="mongodb+srv://cslab:bQt5TU6zQsu_LZ@storehouse.om2d9c0.mongodb.net", 
                             server_api=ServerApi("1"))
db = client["storehouse"]
collection  = db["machines"]
documents = [doc for doc in collection.find({})]
client.close()

# Sort documents by machine name
documents.sort(key=lambda doc: doc["_id"])

# Create cluster store
cluster_store: dict[str, dict] = dict()
for doc in documents:
    cluster_store[doc["_id"]] = {
            "nodes": int(doc["nodes"]),
            "ppn": int(doc["ppn"])
    }
machines = list(cluster_store.keys())

elem_cluster = dbc.Container([

    dcc.Store(id="cluster-store", data=cluster_store),

    dbc.Row([
        dbc.Col([dbc.CardImg(src="../../assets/static/images/cluster.svg")], width=1),
        dbc.Col([
            dbc.Row([html.H5("Cluster"),
                     html.P("""Select by name or manually the specifications of
                            the cluster.""")])
            ], width=10)
    ], class_name="element-header sticky-top"),

    dbc.Row([
        dbc.Button("To manual configuration", id="cluster-btn")
        ], class_name="p-3"),

    dbc.Row([
        dbc.Select(machines, machines[0], id="cluster-machines")
    ], id="cluster-names-display", style={"display": "block"},
            class_name="mx-2"),

    dbc.Row([
        dbc.Col([
            dbc.InputGroup([
                dbc.InputGroupText("nodes", style={"width": "50%"}),
                dbc.Input(type="number", min=0, value=0, id="cluster-nodes")
            ])
        ], style={"display": "inline-block"}, width=6),

        dbc.Col([
            dbc.InputGroup([
                dbc.InputGroupText("ppn", style={"width": "50%"}),
                dbc.Input(type="number", min=0, value=0, id="cluster-ppn")
            ])
        ], style={"display": "inline-block"}, width=6),

    ], id="cluster-manual-display", style={"display": "none"}, class_name="mx-2")

], class_name="element d-flex align-items-strech")

clientside_callback(

        ClientsideFunction(
            namespace="clientside",
            function_name="cluster_events"
        ),

        # Output("cluster-store", "data"),
        Output("cluster-btn", "children"),
        Output("cluster-names-display", "style"),
        Output("cluster-manual-display", "style"),

        Input("cluster-btn", "n_clicks"),
        # Input("cluster-machines", "value"),
        # Input("cluster-nodes", "value"),
        # Input("cluster-ppn", "value")

)

# @callback(
#         Output(component_id="cluster-store", component_property="data"),
#         Output(component_id="cluster-options", component_property="children"),
#         Output(component_id="to-manual-display", component_property="style"),
#         Output(component_id="to-name-display", component_property="style"),
#         Input(component_id="to-manual-btn", component_property="n_clicks"),
#         Input(component_id="to-name-btn", component_property="n_clicks"),
#         Input(component_id="cluster-store", component_property="data"),
#         Input(component_id="cluster-options", component_property="children")
#         )
# def cb_cluster(n1, n2, data, cluster_opts):
# 
#     visible = {"display": "block"}
#     invisible = {"display": "none"}
# 
#     if ctx.triggered_id == "to-manual-btn":
#         if data["nodes"] > 0:
#             nodes = dbc.Col([ 
#                              dbc.Input(type="number", 
#                                        min=0, 
#                                        value=data["nodes"],
#                                        placeholder="number of nodes") 
#                              ])
#         else:
#             nodes = dbc.Col([ 
#                              dbc.Input(type="number", 
#                                        min=0, 
#                                        value=0,
#                                        placeholder="number of nodes") 
#                              ])
# 
#         if data["nodes"] > 0:
#             ppn = dbc.Col([ 
#                            dbc.Input(type="number", 
#                                      min=0, 
#                                      value=data["ppn"],
#                                      placeholder="number of nodes") 
#                            ])
#         else:
#             ppn = dbc.Col([ 
#                            dbc.Input(type="number", 
#                                      min=0,
#                                      value=0,
#                                      placeholder="number of nodes") 
#                            ])
# 
#         manual_opts = dbc.Row([ nodes, ppn ], id="cluster-nodes-ppn")
# 
#         # Save machine name to data
#         data["name"] = cluster_opts[0]["props"]["children"][0]["props"]["value"]
# 
#         return data, [manual_opts], invisible, visible
# 
#     else:
#         if data["name"] != "":
#             name_opts = dbc.Row([
#                 dbc.Select(machines, data["name"])
#                 ], id="cluster-name")
#         else:
#             name_opts = dbc.Row([
#                 dbc.Select(machines, machines[0])
#                 ], id="cluster-name")
#             data["name"] = machines[0]
# 
#         # Save nodes and ppn to data
#         if cluster_opts is not None:
#             nodes_inp, ppn_inp = cluster_opts[0]["props"]["children"]
# 
#             nodes = nodes_inp["props"]["children"][0]["props"]["value"]
#             if nodes is not None:
#                 data["nodes"] = int(nodes)
# 
#             ppn = ppn_inp["props"]["children"][0]["props"]["value"]
#             if ppn is not None:
#                 data["ppn"] = int(ppn)
# 
#         return data, [name_opts], visible, invisible

