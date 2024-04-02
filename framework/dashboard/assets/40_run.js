Object.assign(window.dash_clientside.clientside, {

	run_options: function(sim_type, sim_stop_condition) {
		let visible = {"display": "flex"};
		let invisible = {"display": "none"};

		if (sim_type == "Dynamic") {
			if (sim_stop_condition == "Time")
				return [visible, visible, invisible];
			else
				return [visible, invisible, visible];
		}
		else {
			return [invisible, invisible, invisible];
		}
	},

	run_simulation: 
	function(
		n_clicks, 
		workloads_machine, 
		workloads_suite, 
		generator_type, 
		generator_options, 
		cluster_clicks,
		cluster_data,
		cluster_machine,
		cluster_nodes,
		cluster_ppn,
		schedulers_data,
		schedulers,
		simulation_experiments,
		simulation_type,
		simulation_stop_condition,
		simulation_time_condition,
		generator_time_condition,
		simulation_jobs_condition
	) 
	{
		// Define the return object
		let data = {
			'workloads-machine': workloads_machine,
			'workloads-suite': workloads_suite,
			'generator-type': generator_type,
			'generator-input': undefined,
			'cluster-nodes': undefined,
			'cluster-ppn': undefined,
			'schedulers': undefined,
			'simulation-experiments': simulation_experiments,
			'simulation-type': simulation_type,
			'simulation-stop-condition': undefined
		};

		// Generator options definitions
		if (generator_type == 'Random Generator') {
			let gen_value_component = generator_options.props.children[1];
			data['generator-input'] = gen_value_component.props.value;
		}
		else if (generator_type == 'Dictionary Generator') {
			let gen_options = {};
			let name, value;
			for (inp_group of generator_options) {
				children = inp_group.props.children
				name = children[0].props.children;
				value = children[1].props.value;
				Object.assign(gen_options, {
					[name]: value
				});
			}
			data['generator-input'] = gen_options;
		}
		else return window.dash_clientside.no_update;

		// Cluster definitions
		if (cluster_clicks % 2 == 0 || cluster_clicks === undefined) {
			data['cluster-nodes'] = cluster_data[cluster_machine]['nodes'];
			data['cluster-ppn'] = cluster_data[cluster_machine]['ppn'];
		}
		else {
			data['cluster-nodes'] = cluster_nodes;
			data['cluster-ppn'] = cluster_ppn;
		}

		// Schedulers definitions
		let sched_obj = {};
		let row_children, checkbox, collapse;
		for (row of schedulers) {
			row_children = row.props.children;
			checkbox = row_children[0];
			collapse = row_children[1];


			// If checkbox is checked then walk through
			// the hyperparameters list and push the bundle to the
			// sched_obj
			if (checkbox.props.value === true) {

				let hyperparams = {};
				let inp_group_children, name, value, type;
				for (inp_group of collapse.props.children) {
					inp_group_children = inp_group.props.children;
					name = inp_group_children[0].props.children;
					type = inp_group_children[1].type;

					if (type === 'Input')
						value = inp_group_children[1].props.value;
					else
						value = undefined;

					Object.assign(hyperparams, {
						[name]: value
					});

				}

				// Find module name for scheduling algorithm
				let mod_name;
				for (mod of schedulers_data) {
					if (mod.name == checkbox.props.label) {
						mod_name = mod.module;
						break;
					}
				}

				Object.assign(sched_obj, {
					[mod_name]: hyperparams
				});

			}

		}

		data['schedulers'] = sched_obj;

		// Simulation stop condition definitions
		if (simulation_type == 'Dynamic') {
			if (simulation_stop_condition == 'Time')
				data['simulation-stop-condition'] = {
					[simulation_stop_condition]: {
						"Stop time": simulation_time_condition,
						"Generator time": generator_time_condition
					}
				};
			else
				data['simulation-stop-condition'] = {
					[simulation_stop_condition]: simulation_jobs_condition
				};
		}

		return data;

	}

})
