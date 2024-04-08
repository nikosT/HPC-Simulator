Object.assign(window.dash_clientside.clientside, {

	create_results_menu:  function(data) {

		let menu_children = [];

		let all_btn = {
			'type': 'Button',
			'namespace': 'dash_bootstrap_components',
			'props': {
				'children': 'All in one',
				'style': {'width': '100%', 'margin': '5px'},
				'href': '#all-experiments'
			}
		};

		menu_children.push(all_btn);

		let index = 0;
		for ([exp_name, exp_obj] of Object.entries(data)) {

			let exp_btn = {
				'type': 'Button',
				'namespace': 'dash_bootstrap_components',
				'props': {
					'id': {
						'type': 'results-exp-btn',
						'index': index
					},
					'children': exp_name,
					'style': {'width': '100%', 'margin': '5px'},
					'color': 'secondary'
				}
			}

			menu_children.push(exp_btn);

			let exp_collapse_children = [];

			for ([sched_name, res_obj] of Object.entries(exp_obj)) {

				exp_id = exp_name.toLowerCase();
				exp_id = exp_id.replace(/\s+/g, '_');

				sched_id = sched_name.toLowerCase();
				sched_id = sched_id.replace(/\s+/g, '_');

				unique_id = exp_id + '~' + sched_id;

				let label = {
					'type': 'Label',
					'namespace': 'dash_bootstrap_components',
					'props': {
						'children': sched_name,
						'style': {'width': '100%'}
					}
				}

				let resource_usage_btn = {
					'type': 'Button',
					'namespace': 'dash_bootstrap_components',
					'props': {
						'children': 'Resource usage',
						'style': {'width': '100%'},
						'color': 'primary',
						'outline': true,
						'href': '#' + unique_id + '~resource_usage'
					}
				}

				let jobs_utilization_btn = {
					'type': 'Button',
					'namespace': 'dash_bootstrap_components',
					'props': {
						'children': 'Jobs utilization',
						'style': {'width': '100%'},
						'color': 'primary',
						'outline': true,
						'href': '#' + unique_id + '~jobs_utilization'
					}
				}

				if (sched_name == 'Default Scheduler') {
					exp_collapse_children.push({
						'type': 'Container',
						'namespace': 'dash_bootstrap_components',
						'props': {
							'children': [
								label,
								resource_usage_btn
							]
						}
					})
				}
				else {
					exp_collapse_children.push({
						'type': 'Container',
						'namespace': 'dash_bootstrap_components',
						'props': {
							'children': [
								label,
								resource_usage_btn,
								jobs_utilization_btn
							]
						}
					})
				}
				
			}

			menu_children.push({
				'type': 'Collapse',
				'namespace': 'dash_bootstrap_components',
				'props': {
					'id': {
						'type': 'results-exp-collapse',
						'index': index
					},
					'children': exp_collapse_children,
					'style': {
						'width': '100%', 
						'margin': '5px',
						'padding': '2px',
						'border-style': 'solid', 
						'border-width': '2px',
						'border-radius': '5px'
					},
					'is_open': false
				}
			})


			// Increment the index
			index++;

		}


		return menu_children;

	},

	open_results_nav: function(n_clicks) {
		return true;
	},

	open_experiment_collapse: function(n_clicks) {
		if (n_clicks % 2 == 1)
			return true;
		else 
			return false;
	},

	create_graph: function(hash, data) {

		if (data === undefined)
			return window.dash_clientside.no_update;

		let selection = hash.replace('#', '');
		let arr = selection.split('~')

		if (arr.length == 1 && arr[0] == 'all-experiments') {
			this.create_all_graph(data);
		}
		else {

			if (arr.length != 3)
				return window.dash_clientside.no_update;

			[exp_tag, sched_tag, graph_tag] = arr;

			experiment = exp_tag.replace(/_/g, ' ')
			experiment = experiment.charAt(0).toUpperCase() + experiment.slice(1)

			scheduler = ''
			for (sched_name of Object.keys(data[experiment])) {
				sched_name_tagged = sched_name.toLowerCase().replace(/\s+/g, '_')
				if (sched_name_tagged == sched_tag) {
					scheduler = sched_name;
					break;
				}
			}

			graph = graph_tag.replace(/_/g, ' ')
			graph = graph.charAt(0).toUpperCase() + graph.slice(1)

			// Return figure
			if (graph == "Resource usage") {
				Plotly.newPlot("results-graph", JSON.parse(data[experiment][scheduler][graph]), {'displayModeBar': false})
			}
			else if (graph == "Jobs utilization") {

				let job_names = [];
				let speedup = [];
				let turnaround = [];
				let waiting = [];

				for ([job_name, job_util] of Object.entries(data[experiment][scheduler][graph])) {
					job_names.push(job_name);
					speedup.push(job_util['speedup']);
					turnaround.push(job_util['turnaround']);
					waiting.push(job_util['waiting']);
				}

				let speedup_trace = {
					'y': speedup,
					'text': job_names,
					'name': 'Speedup',
					'boxpoints': 'all',
					'jitter': 0.2,
					'boxmean': 'sd',
					'type': 'box',
					'showlegend': false
				};

				let turnaround_trace = {
					'y': turnaround,
					'text': job_names,
					'name': 'Turnaround ratio',
					'boxpoints': 'all',
					'jitter': 0.2,
					'boxmean': 'sd',
					'type': 'box',
					'showlegend': false,
					'xaxis': 'x2',
					'yaxis': 'y2'
				};

				let waiting_trace = {
					'y': waiting,
					'text': job_names,
					'name': 'Waiting time difference',
					'boxpoints': 'all',
					'jitter': 0.2,
					'boxmean': 'sd',
					'type': 'box',
					'showlegend': false,
					'xaxis': 'x3',
					'yaxis': 'y3'
				};

				let traces = [speedup_trace, turnaround_trace, waiting_trace];

				let layout = {
					title: '<b>' + scheduler + '</b><br>Jobs utilization',
					grid: {
						rows: 1,
						columns: 3,
						pattern: 'independent'
					}
				}

				Plotly.newPlot('results-graph', traces, layout, {'displayModeBar': false});
			}
		}
	},

	create_all_graph: function(data) {

		// Get all the scheduling algorithms name except the default one
		schedulers = Object.keys( data["Experiment 0"] )
		console.log(schedulers)
		schedulers.splice(schedulers.indexOf('Default Scheduler'), 1)

		let aggr_exps = 0;
		let aggr_jobs = 0;
		let traces = [];
		let avg_makespans = [];

		for (scheduler of schedulers) {

			aggr_exps = 0;

			let index = 0;
			let jobs_text = [];
			let jobs_speedups = [];
			let jobs_turnaround = [];
			let jobs_waiting = [];
			let makespans = [];

			for (exp_obj of Object.values(data)) {

				aggr_jobs = 0;
				aggr_exps++;

				exp_data = exp_obj[scheduler]['Jobs utilization'];

				for ([job_name, job_util] of Object.entries(exp_data)) {
					jobs_text.push('[exp' + index.toString() + ']' + job_name);
					jobs_speedups.push(job_util['speedup']);
					jobs_turnaround.push(job_util['turnaround']);
					jobs_waiting.push(job_util['waiting']);

					// Culminative counter of jobs
					aggr_jobs++;
				}

				makespans.push(exp_obj[scheduler]['Makespan speedup']);

				// Increment index
				index++;
				
			}

			let speedups_trace = {
				'y': jobs_speedups,
				'text': jobs_text,
				'name': scheduler,
				'boxpoints': 'all',
				'jitter': 0.2,
				'boxmean': 'sd',
				'type': 'box',
				'showlegend': false,
				'marker': {
					'opacity': 0.6
				}
			};

			let turnaround_trace = {
				'y': jobs_turnaround,
				'text': jobs_text,
				'name': scheduler,
				'boxpoints': 'all',
				'jitter': 0.2,
				'boxmean': 'sd',
				'type': 'box',
				'showlegend': false,
				'xaxis': 'x2',
				'yaxis': 'y2',
				'marker': {
					'opacity': 0.6
				}
			};

			let waiting_trace = {
				'y': jobs_waiting,
				'text': jobs_text,
				'name': scheduler,
				'boxpoints': 'all',
				'jitter': 0.2,
				'boxmean': 'sd',
				'type': 'box',
				'showlegend': false,
				'xaxis': 'x3',
				'yaxis': 'y3',
				'marker': {
					'opacity': 0.6
				}
			};

			traces.push(speedups_trace);
			traces.push(turnaround_trace);
			traces.push(waiting_trace);

			// Calculate average makespan speedup for scheduling
			// algorithm
			let sum = 0;
			makespans.forEach(mkspan_speedup => {
				sum += mkspan_speedup;
			});

			if (makespans.length > 0)
				avg_makespans.push(sum / makespans.length);
			else
				avg_makespans.push(0);

		}

		// Create trace for makespan speedups
		let makespans_trace = {
			'type': 'scatter',
			'y': avg_makespans,
			'x': schedulers,
			'name': 'Average makespan speedup',
			'marker': {
				'color': 'black'
			},
			'showlegend': false
		};

		// Add trace for average makespan speedups
		traces.push(makespans_trace);

		// Add makespan speedups for annotation
		let annotations = [];

		for (i in avg_makespans) {
			annotations.push({
				'x': i,
				'y': avg_makespans[i],
				'xref': 'x',
				'yref': 'y',
				'text': '<b>' + avg_makespans[i].toFixed(2) + '</b>',
				'font': {
					'size': 18
				}
			})
		}

		// Create update visibility for traces args
		let upd_all_traces = [];
		let upd_speedup_traces = [];
		let upd_turnaround_traces = [];
		let upd_waiting_traces = [];

		for (s in schedulers) {

			upd_all_traces.push(true);
			upd_all_traces.push(true);
			upd_all_traces.push(true);

			upd_speedup_traces.push(true);
			upd_speedup_traces.push(false);
			upd_speedup_traces.push(false);

			upd_turnaround_traces.push(false);
			upd_turnaround_traces.push(true);
			upd_turnaround_traces.push(false);

			upd_waiting_traces.push(false);
			upd_waiting_traces.push(false);
			upd_waiting_traces.push(true);
		}

		upd_all_traces.push(true);
		upd_speedup_traces.push(true);
		upd_turnaround_traces.push(false);
		upd_waiting_traces.push(false);

		let update_menu = {
			'buttons': [
				{
					'args': [
						{
							'visible': upd_all_traces,
							'xaxis': ['x', 'x2', 'x3'],
							'yaxis': ['y', 'y2', 'y3']
						},
						{
							'xaxis.domain': [0, 0.3],
							'xaxis.visible': true,
							'yaxis.visible': true,

							'xaxis2.domain': [0.35, 0.65],
							'xaxis2.visible': true,
							'yaxis2.visible': true,

							'xaxis3.domain': [0.7, 1],
							'xaxis3.visible': true,
							'yaxis3.visible': true,

							'annotations': annotations
						}
					],
					'label': 'All',
					'method': 'update'
				},
				{
					'args': [
						{
							'visible': upd_speedup_traces,
							'xaxis': ['x', 'x', 'x'],
							'yaxis': ['y', 'y', 'y']
						},
						{
							'xaxis.domain': [0, 1],
							'xaxis.visible': true,
							'yaxis.visible': true,

							'xaxis2.visible': false,
							'xaxis2.domain': [0, 0],
							'yaxis2.visible': false,

							'xaxis3.visible': false,
							'xaxis3.domain': [0, 0],
							'yaxis3.visible': false,

							'annotations': annotations
						}
					],
					'label': 'Jobs speedups',
					'method': 'update'
				},
				{
					'args': [
						{
							'visible': upd_turnaround_traces,
							'xaxis': ['x2', 'x2', 'x2'],
							'yaxis': ['y2', 'y2', 'y2']
						},
						{
							'xaxis.visible': false,
							'xaxis.domain': [0, 0],
							'yaxis.visible': false,

							'xaxis2.visible': true,
							'xaxis2.domain': [0, 1],
							'yaxis2.visible': true,

							'xaxis3.visible': false,
							'xaxis3.domain': [0, 0],
							'yaxis3.visible': false,

							'annotations': []
						}
					],
					'label': 'Jobs turnarounds',
					'method': 'update'
				},
				{
					'args': [
						{
							'visible': upd_waiting_traces,
							'xaxis': ['x3', 'x3', 'x3'],
							'yaxis': ['y3', 'y3', 'y3']
						},
						{
							'xaxis.domain': [0, 0],
							'xaxis.visible': false,
							'yaxis.visible': false,

							'xaxis2.visible': false,
							'xaxis2.domain': [0, 0],
							'yaxis2.visible': false,

							'xaxis3.visible': true,
							'xaxis3.domain': [0, 1],
							'yaxis3.visible': true,

							'annotations': []
						}
					],
					'label': 'Jobs waiting time',
					'method': 'update'
				},
			],
			'direction': 'left',
			'showactive': true,
			'type': 'buttons',
			'x': 1,
			'xanchor': 'right',
			'y': 1,
			'yanchor': 'bottom',
		};

		let suffix = aggr_exps + ' experiment(s), ' + aggr_jobs + ' jobs per experiment';
		let layout = {
			title: '<b>All experiments and scheduling algorithms</b><br>' + suffix,
			grid: {
				rows: 1,
				columns: 3,
				pattern: 'independent'
			},
			xaxis: {
				title: {
					text: '<b>Scheduling algorithms</b>', 
					font: {size: 18}
				},
				tickfont: {size: 16}
			},
			yaxis: {
				title: {
					text: '<b>Jobs and average makespan speedups</b>',
					font: {size: 18}
				},
				tickfont: {size: 16}
			},
			xaxis2: {
				title: {
					text: '<b>Scheduling algorithms</b>',
					font: {size: 18}
				},
				tickfont: {size: 16}
			},
			yaxis2: {
				title: {
					text: '<b>Jobs turnaround ratio</b>',
					font: {size: 18}
				},
				tickfont: {size: 16}
			},
			xaxis3: {
				title: {
					text: '<b>Scheduling algorithms</b>',
					font: {size: 18}
				},
				tickfont: {size: 16}
			},
			yaxis3: {
				title: {
					text: '<b>Jobs waiting time difference</b>',
					font: {size: 18}
				},
				tickfont: {size: 16}
			},
			updatemenus: [update_menu],
			annotations: annotations
		};

		Plotly.newPlot("results-graph", traces, layout, {'autosizable': true, 'displayModeBar': false});

	},

	results_nav_isopen: function(isopen) {
		if (isopen)
			return {'filter': 'blur(2px) grayscale(50%)'};
		else
			return {};
	},

	results_modal_isopen: function(n_clicks, m_clicks) {

		let triggered = dash_clientside.callback_context.triggered[0]

		if (triggered === undefined)
			return window.dash_clientside.no_update;

		let triggered_id = triggered['prop_id'].split('.')[0]

		if (triggered_id == 'results-close-btn')
			return false;
		else
			return true;
	},

	results_download_graph: function(n_clicks) {
		graph_elem = document.getElementById('results-graph');
		updatemenus = graph_elem.layout.updatemenus;
		graph_elem.layout.updatemenus = [];

		Plotly.downloadImage('results-graph', {
			'format': 'svg',
			'height': 1080,
			'width': 1920,
			'filename': 'newplot'
		});
		
		graph_elem.layout.updatemenus = updatemenus;
	}

})
