from dash.exceptions import PreventUpdate
import plotly.graph_objects as go
from dash import callback, Input, Output, State, html, dcc, ctx
import dash_bootstrap_components as dbc
import jsonpickle
import inspect
import base64
from numpy import average as avg
from numpy import median as med
import zipfile
from concurrent.futures import ProcessPoolExecutor

from layouts.elements.generator import elem_generator
from layouts.elements.cluster import elem_cluster
from layouts.elements.schedulers import elem_schedulers

# TODO: Get specs from DB rather than SPECS python file
import os
import sys
sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../../"
    )))
from realsim.cluster.specs import SPECS

sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../"
    )))

from experiment import Experiment

main_data = {"figures": dict(), "list of jobs": dict(), "runs": 0}

main_layout = dbc.Container([

    # STORE
    dcc.Store(id="main-store", data=main_data, storage_type="memory"),

    # EXPORT FIGURES MODAL
    dbc.Modal([
        dbc.ModalHeader([dbc.ModalTitle("Save figures")]),
        dbc.ModalBody([
            dbc.Row([
                dbc.InputGroup([
                    dbc.InputGroupText("Height", style={"width": "25%"}),
                    dbc.Input(type="number", min=1, value=1, id="figures-height"),
                    dbc.InputGroupText("px")
                    ], class_name="my-2")
                ]),
            dbc.Row([
                dbc.InputGroup([
                    dbc.InputGroupText("Width", style={"width": "25%"}),
                    dbc.Input(type="number", min=1, value=1, id="figures-width"),
                    dbc.InputGroupText("px")
                    ], class_name="my-2")
                ]),
            dbc.Row([
                dbc.Select(["jpg", "png", "svg", "pdf", "html", "json"], "jpg",
                           id="figures-format")
                ]),
            dbc.Row([
                dcc.Download(id="download-figures"),
                dbc.Button("Save figures", id="save-figures-btn",
                           class_name="my-2",
                           style={"width": "60%"})
                ], justify="center")
            ])
        ], id="export-figures-modal", is_open=False, centered=True),


    # EXPORT LIST OF JOBS MODAL

    # RESULTS
    dbc.Spinner([
        dbc.Modal([
            dbc.ModalHeader([
                dbc.ModalTitle("Experiment Results"),
                dbc.Button("Export figures", class_name="mx-4",
                           id="export-figures-btn"),
                dbc.Button(["Export list of jobs",
                            dcc.Download(id="download-jobs")], id="export-jobs")
            ]),
            dbc.ModalBody([
                dbc.Tabs(id="figures", class_name="d-flex flex-nowrap",
                         style={"overflow": "auto", "white-space": "nowrap"})
            ])
            ], is_open=False, id="modal", fullscreen=True)
    ], fullscreen=True, type="grow", color="primary"),

    # DASHBOARD
    dbc.Row([
        dbc.Col([ elem_generator ], class_name="py-3", width=6),
        dbc.Col([ elem_cluster ], class_name="p-3 align-self-strech", width=6),
        ], class_name="align-items-center justify-content-center h-50",
            style={"height": "45%"}),

    dbc.Row([
        dbc.Col([ elem_schedulers ], class_name="p-3", width=6),
        dbc.Col([
            dbc.Row([
                dbc.InputGroup([
                    dbc.InputGroupText("Number of experiments"),
                    dbc.Input(type="number", min=1, value=1, id="num-of-experiments")
                ]),
            ]),
            dbc.Row([
                dbc.Col([
                    dbc.Button("Run simulation",
                               id="run-simulation",
                               style={"width": "100%", "height": "45vh",
                                      "border-radius": "10px"}
                               )
                    ], width=6),
                dbc.Col([
                    dbc.Button("View results",
                               id="view-results",
                               class_name="btn-success",
                               style={"width": "100%", "height": "45vh",
                                      "border-radius": "10px"}
                               )
                    ], width=6),
                ])
            ])
        ], class_name="align-items-center justify-content-center", 
            style={"height": "45%"})

    ], class_name="flex-row h-100", fluid=True, style={"height": "100vh"})


def parallel_experiments(par_inp):

    num, generator, gen_input, nodes, ppn, schedulers = par_inp
    
    # Create set of jobs
    jobs_set = generator.generate_jobs_set(gen_input)

    # Create an experiment based on the above
    exp = Experiment(jobs_set, nodes, ppn, schedulers)
    exp.set_default("Default Scheduler")
    
    # Fire the simulation for this experiment
    exp.run()

    # Draw the figures
    figures = exp.plot()

    return {f"exp{num}": jsonpickle.encode(jobs_set)}, {f"exp{num}": figures}

@callback(
        Output(component_id="main-store", component_property="data"),
        Output(component_id="modal", component_property="is_open"),
        Output(component_id="figures", component_property="children"),
        Input(component_id="run-simulation", component_property="n_clicks"),
        Input(component_id="view-results", component_property="n_clicks"),
        State(component_id="main-store", component_property="data"),
        State(component_id="generator-store", component_property="data"),
        State(component_id="generator-options", component_property="children"),
        State(component_id="cluster-store", component_property="data"),
        State(component_id="cluster-options", component_property="children"),
        State(component_id="schedulers-checkboxes", component_property="value"),
        State(component_id="schedulers-store", component_property="data"),
        State(component_id="num-of-experiments", component_property="value"),
        prevent_initial_call=True
        )
def cb_run_simulation(n1, n2, 
                      main_data,
                      gen_data, 
                      gen_opts, 
                      cluster_data,
                      cluster_opts,
                      sched_checks, 
                      sched_data,
                      num_of_exps):

    if ctx.triggered_id == "run-simulation":

        # Cleanup the figures store
        main_data["figures"] = dict()

        # Cleanup list of jobs store
        main_data["list of jobs"] = dict()

        # Increase the runs counter
        if "runs" not in list(main_data.keys()):
            main_data["runs"] = 1
        else:
            main_data["runs"] += 1

        # Get the selected generator
        generator = jsonpickle.decode( gen_data["generator"] )

        # Get the input type of the generator
        inp_sig = inspect.signature(generator.generate_jobs_set).parameters['arg']
        inp_type = inp_sig.annotation

        # Based on the type gather the input from the generator-options Div
        # to build the input for the instance
        gen_input = None
        if inp_type == int:
            # RANDOM
            input_group = gen_opts[0]
            _, gr_value = input_group["props"]["children"]
            gen_input = int( gr_value["props"]["value"] )
        elif inp_type == dict[str, int]:
            # DICTIONARY
            gen_input = dict()
            for input_group in gen_opts:

                gr_text, gr_inp = input_group["props"]["children"]
                name = str(gr_text["props"]["children"])
                value = gr_inp["props"]["value"]

                gen_input[name] = value
        else:
            # LIST
            content = gen_opts[0]["props"]["contents"]
            content = content.replace("data:text/plain;base64,", "")
            content = str(base64.b64decode(content), "utf-8")

            gen_input = list()
            loads = list(filter(lambda name: name != "", content.split("\n")))
            for load in loads:
                gen_input.append(load)

        # Get cluster specs
        if cluster_opts[0]["props"]["id"] == "cluster-name":
            specs = SPECS[cluster_data["name"]]
            nodes = specs["nodes"]
            ppn = specs["ppn"]
        else:
            # Check for the values inside the Input elements
            nodes_inp, ppn_inp = cluster_opts[0]["props"]["children"]
            nodes = nodes_inp["props"]["children"][0]["props"]["value"]
            ppn = ppn_inp["props"]["children"][0]["props"]["value"]

        # Get the scheduling classes
        schedulers = [jsonpickle.decode( sched_data[number] )
                      for number in sched_checks]

        # Calculate the number of parallel processes based on experiments
        parallel_procs = int( os.cpu_count() / len(schedulers) )
        if parallel_procs == 0:
            parallel_procs = 1

        # Create a parallel executor
        executor = ProcessPoolExecutor(max_workers=os.cpu_count())

        futures = list()
        for i in range(num_of_exps):
            futures.append(executor.submit(parallel_experiments, (i, generator, 
                                                                  gen_input, 
                                                                  nodes, ppn,
                                                                  schedulers))
                           )

        executor.shutdown(wait=True)

        for future in futures:

            res = future.result()

            jobs_dict, figures_dict = res
            main_data["list of jobs"].update(jobs_dict)
            main_data["figures"].update(figures_dict)

        # for i in range(num_of_exps):
        #     # Create set of jobs
        #     jobs_set = generator.generate_jobs_set(gen_input)
        #     # Store the list of jobs
        #     main_data["list of jobs"].update({f"exp{i}":
        #                                       jsonpickle.encode(jobs_set )
        #                                       })

        #     # Create an experiment based on the above
        #     exp = Experiment(jobs_set, nodes, ppn, schedulers)
        #     exp.set_default("Default Scheduler")
        #     
        #     # Fire the simulation for this experiment
        #     exp.run()

        #     # Draw the figures
        #     figures = exp.plot()

        #     # For each figure save them under a specific experiment number
        #     main_data["figures"].update({f"exp{i}": figures})

        # Create the tabs
        tabs = list()
        # for exp_name, fig_dict in main_data["figures"].items():
        #     for fig_name, fig in fig_dict.items():
        #         tabs.append(dbc.Tab([
        #             dcc.Graph(figure=fig, 
        #                       style={"height": "80vh"})
        #             ], label=f"[{exp_name}] {fig_name}", 
        #                             class_name="flex-nowrap"))

        # Create the overall speedups
        new_makespan_line = dict()
        new_boxes = dict()
        for i in range(len(schedulers) - 1):
            new_makespan_line[i] = []
            new_boxes[i] = {
                    "boxmean": "sd",
                    "boxpoints": "all",
                    "name": "speedup",
                    "showlegend": False,
                    "x": [],
                    "y": [],
                    "text": []
            }

        for exp_name, fig_dict in main_data["figures"].items():
            data = fig_dict["Speedups"].data
            for fig_data in data:
                if type(fig_data) == go.Box:
                    # The x value will be the key for the new box
                    x = fig_data.x
                    key = int(x[0])
                    y = fig_data.y

                    # Translate text to display the experiment
                    text = fig_data.text
                    text = [f"[{exp_name}]{txt}" for txt in text]

                    new_boxes[key]["x"].extend(x)
                    new_boxes[key]["y"].extend(y)
                    new_boxes[key]["text"].extend(text)

                if type(fig_data) == go.Scatter:
                    for x, y in enumerate(fig_data.y):
                        new_makespan_line[x].append(y)

        fig = go.Figure()

        # New boxplots
        for box_data in new_boxes.values():
            fig.add_trace(go.Box( box_data ))

        # New makespan average speedup line
        y_val = list()
        for values in new_makespan_line.values():
            y_val.append( avg(values) )

        fig.add_trace(go.Scatter(
            x=list(new_makespan_line.keys()),
            y=y_val,
            mode="lines+markers",
            marker=dict(color="black"), 
            name="Average Makespan Speedup"
        ))

        fig.add_hline(y=1, line_color="black", line_dash="dot")

        layout = main_data["figures"]["exp0"]["Speedups"].layout
        layout.annotations = []
        fig.layout = layout
        fig.layout.title = f"""<b>Average makespan and per job speedups for {num_of_exps} experiments with {len( jsonpickle.decode(main_data['list of jobs']['exp0']) )} jobs each</b>"""

        # Show AvgMakespanSpeedup statically on plot
        for i, y in enumerate(y_val):
            fig.add_annotation(text=f"<b>{round(y, 3)}</b>", 
                               font=dict(size=14),
                               x=i, 
                               y=y,
                               arrowcolor="black")

            # max_y = max(new_boxes[i]["y"])
            # min_y = min(new_boxes[i]["y"])
            # mean_y = avg(new_boxes[i]["y"])
            # med_y = med(new_boxes[i]["y"])

            # fig.add_annotation(text=f"<b>{round(max_y, 3)}</b>",
            #                    font=dict(color="gray"),
            #                    x=i, 
            #                    y=max_y, 
            #                    arrowcolor="gray")

            # fig.add_annotation(text=f"<b>{round(min_y, 3)}</b>",
            #                    font=dict(color="gray"),
            #                    x=i, 
            #                    y=min_y, 
            #                    arrowcolor="gray")

            # fig.add_annotation(text=f"<b>{round(mean_y, 3)}</b>",
            #                    font=dict(color="gray"),
            #                    x=i-0.02, 
            #                    y=mean_y, 
            #                    arrowcolor="gray")

            # fig.add_annotation(text=f"<b>{round(med_y, 3)}</b>", 
            #                    font=dict(color="gray"),
            #                    x=i-0.1, 
            #                    y=med_y, 
            #                    arrowcolor="gray")

        # Save all experiments speedups figure
        main_data["figures"].update({"all": fig})

        tabs.insert(0, dbc.Tab([
            dcc.Graph(figure=fig, style={"height": "80vh"})
            ], label="All speedups", class_name="flex-nowrap"))

        return main_data, True, tabs

    elif ctx.triggered_id == "view-results":
        tabs = list()
        if main_data["figures"] == {}:
            return main_data, False, tabs
        else:
            for exp_name, fig_dict in main_data["figures"].items():

                if exp_name == "all":
                    tabs.insert(0, dbc.Tab([
                        dcc.Graph(figure=fig_dict, style={"height": "80vh"})
                        ], label="All speedups", class_name="flex-nowrap")
                    )
                    continue

                # for fig_name, fig in fig_dict.items():
                #     tabs.append(dbc.Tab([
                #         dcc.Graph(figure=fig, 
                #                   style={"height": "80vh"})
                #         ], label=f"[{exp_name}] {fig_name}",
                #                         class_name="flex-nowrap"))
            return main_data, True, tabs
    else:
        return main_data, False, []

@callback(
        Output(component_id="export-figures-modal", component_property="is_open"),
        Output(component_id="download-figures", component_property="data"),
        Input(component_id="export-figures-btn", component_property="n_clicks"),
        Input(component_id="save-figures-btn", component_property="n_clicks"),
        State(component_id="app-store", component_property="data"),
        State(component_id="main-store", component_property="data"),
        State(component_id="figures-height", component_property="value"),
        State(component_id="figures-width", component_property="value"),
        State(component_id="figures-format", component_property="value"),
        prevent_initial_call=True
        )
def save_figures(n1, n2, 
                 app_data, 
                 main_data, 
                 fig_height, 
                 fig_width, 
                 fig_format):

    if ctx.triggered_id == "export-figures-btn":
        return True, None
    elif ctx.triggered_id == "save-figures-btn":

        if not os.path.exists("./results"):
            os.mkdir("./results")

        session_dir = f"./results/{app_data['sid']}"
        if not os.path.exists(session_dir):
            os.mkdir(session_dir)

        run_dir = f"{session_dir}/run{main_data['runs']}"
        if not os.path.exists(run_dir):
            os.mkdir(run_dir)

        # Delete zip file if it already exists
        if os.path.exists(f"{run_dir}/figures.zip"):
            os.unlink(f"{run_dir}/figures.zip")

        # Create a zip file
        compressed = zipfile.ZipFile(f"{run_dir}/figures.zip", mode="a")

        for exp_name, fig_dict in main_data["figures"].items():

            if exp_name == "all":
                fig_name = exp_name.lower()
                fig_name = exp_name.replace(":", "_").replace(" ", "_")

                fig = go.Figure(fig_dict)

                if fig_format == "html":
                    fig.write_html(f"{run_dir}/{fig_name}.html")
                elif fig_format == "json":
                    fig.write_json(f"{run_dir}/{fig_name}.json")
                else:
                    fig.write_image(f"{run_dir}/{fig_name}.{fig_format}",
                                    height=fig_height, width=fig_width,
                                    format=fig_format)

                compressed.write(f"{run_dir}/{fig_name}.{fig_format}", 
                                 f"{run_dir}/{fig_name}.{fig_format}",
                                 zipfile.ZIP_DEFLATED)
                continue

            exp_dir = f"{run_dir}/{exp_name}"
            if not os.path.exists(exp_dir):
                os.mkdir(exp_dir)

            fig_dir = f"{exp_dir}/figures"
            if not os.path.exists(fig_dir):
                os.mkdir(fig_dir)

            for fig_name, fig in fig_dict.items():
                fig_name = fig_name.lower()
                fig_name = fig_name.replace(":", "_").replace(" ", "_")

                fig = go.Figure(fig)

                if fig_format == "html":
                    fig.write_html(f"{fig_dir}/{fig_name}.html")
                elif fig_format == "json":
                    fig.write_json(f"{fig_dir}/{fig_name}.json")
                else:
                    fig.write_image(f"{fig_dir}/{fig_name}.{fig_format}",
                                    height=fig_height, width=fig_width,
                                    format=fig_format)
                
                compressed.write(f"{fig_dir}/{fig_name}.{fig_format}",
                                 f"{fig_dir}/{fig_name}.{fig_format}",
                                 zipfile.ZIP_DEFLATED)

        compressed.close()

        zip_uri = f"{run_dir}/figures.zip"
        return False, dcc.send_file(zip_uri)
    else:
        return False, None

@callback(
        Output(component_id="download-jobs", component_property="data"),
        Input(component_id="export-jobs", component_property="n_clicks"),
        State(component_id="app-store", component_property="data"),
        State(component_id="main-store", component_property="data"),
        prevent_initial_call=True
        )
def save_list_of_jobs(n, app_data, main_data):

    if not os.path.exists("./results"):
        os.mkdir("./results")

    session_dir = f"./results/{app_data['sid']}"
    if not os.path.exists(session_dir):
        os.mkdir(session_dir)

    run_dir = f"{session_dir}/run{main_data['runs']}"
    if not os.path.exists(run_dir):
        os.mkdir(run_dir)

    # Delete zip file if it already exists
    if os.path.exists(f"{run_dir}/lists-of-jobs.zip"):
        os.unlink(f"{run_dir}/lists-of-jobs.zip")

    # Create a zip file
    compressed = zipfile.ZipFile(f"{run_dir}/lists-of-jobs.zip", mode="a")

    for exp_name, encoded_jobs_set in main_data["list of jobs"].items():

        exp_dir = f"{run_dir}/{exp_name}"
        if not os.path.exists(exp_dir):
            os.mkdir(exp_dir)
    
        jobs_set = jsonpickle.decode( encoded_jobs_set )

        fd = open(f"{exp_dir}/jobs.txt", "w")
        fd.writelines([f"{job.job_name}\n" for job in jobs_set])
        fd.close()

        compressed.write(f"{exp_dir}/jobs.txt", f"{exp_dir}/jobs.txt",
                         zipfile.ZIP_DEFLATED)

    compressed.close()

    return dcc.send_file(f"{run_dir}/lists-of-jobs.zip")
