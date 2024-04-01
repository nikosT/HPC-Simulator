window.dash_clientside = Object.assign({}, window.dash_clientside, {
	clientside: {

		create_schedulers_checklist: function(data) {

			let children = [];

			for (i = 0; i < data.length; i++) {

				let checkbox= {
					'type': 'Checkbox',
					'namespace': 'dash_bootstrap_components',
					'props': {
						'id': {
							'type': 'schedulers-checkboxes',
							'index': JSON.stringify(i)
						},
						'label': data[i].name,
						'disabled': data[i].disabled,
						'value': data[i].selected
					}
				};

				let collapse_children = [];

				if ('hyperparams' in data[i]) {

					for (let [name, value] of Object.entries(data[i].hyperparams)) {
						// console.log(value)
						let name_inp = {
							'type': 'InputGroupText',
							'namespace': 'dash_bootstrap_components',
							'props': {
								'children': [name],
								'style': {
									'width': '50%'
								}
							}
						};

						let val_inp;
						if (value.includes('float')) {
							val_inp = {
								'type': 'Input',
								'namespace': 'dash_bootstrap_components',
								'props': {
									'min': 0.1,
									'value': 1,
									'type': 'number',
									'style': {
										'width': '50%'
									}
								}
							};
						}
						else {
							val_inp = {
								'type': 'Button',
								'namespace': 'dash_bootstrap_components',
								'props': {
									'children': ['Upload'],
									'style': {
										'width': '50%'
									}
								}
							};
						}

						let param = {
							'type': 'InputGroup',
							'namespace': 'dash_bootstrap_components',
							'props': {
								'children': [name_inp, val_inp]
							}
						}

						collapse_children.push(param);

					}
				}


				let collapse = {
					'type': 'Collapse',
					'namespace': 'dash_bootstrap_components',
					'props': {
						'id': {
							'type': 'schedulers-collapse',
							'index': JSON.stringify(i)
						},
						'children': collapse_children,
						'is_open': false
					}
					
				};

				let input_group = {
					'type': 'Row',
					'namespace': 'dash_bootstrap_components',
					'props': {
						'children': [checkbox, collapse],
						'class_name': 'element-item m-1 p-2'
					}
				};

				children.push(input_group);

			}

			return children;
		},

		show_scheduler_hyperparameters: function(value) {
			return value;
		},


		workloads_events: function(n_clicks, m_value, s_options, s_value, data) {

			let visible = {'display': 'block'};
			let invisible = {'display': 'none'};
			let button_classes = ['bi bi-database', 'bi bi-upload'];
			let triggered = dash_clientside.callback_context.triggered[0]

			if (triggered === undefined)
				return window.dash_clientside.no_update;

			let triggered_id = triggered['prop_id'].split('.')[0]

			if (triggered_id == 'workloads-btn') {
				if (n_clicks % 2 == 0) {
					return [button_classes[0], 
						m_value, 
						Object.keys(data[m_value]), 
						"All",
						visible, 
						visible, 
						invisible];
				}
				else {
					return [button_classes[1], 
						m_value, 
						s_options, 
						s_value,
						invisible, 
						invisible, 
						visible];
				}
			}
			else {
				return [button_classes[0], 
					m_value, 
					Object.keys(data[m_value]), 
					"All", 
					visible, 
					visible, 
					invisible];
			}
		},

		generator_options: function(gen_value, machine, suite, data) {
			/*
			 * Return should be the children under the Options
			 * element item in Generator
			 * */

			// Get id of actor who triggered the event
			let triggered = dash_clientside.callback_context.triggered[0]
			if (triggered === undefined)
				return window.dash_clientside.no_update;
			let triggered_id = triggered['prop_id'].split('.')[0];

			if (triggered_id == 'generator-type') {
				if (gen_value == 'List Generator') {
					return {
						'type': 'Button',
						'namespace': 'dash_bootstrap_components',
						'props': {
							'children': 'Upload file',
							'style': {
								'width': '100%'
							}
						}
					};
				}
				else if (gen_value == 'Dictionary Generator') {

					let children = [];

					for (workload of data[machine][suite]) {
						let inpgtext = {
							'type': 'InputGroupText',
							'namespace': 'dash_bootstrap_components',
							'props': {
								'children': workload,
								'style': {
									'width': '50%'
								}
							}
						};

						let inpval = {
							'type': 'Input',
							'namespace': 'dash_bootstrap_components',
							'props': {
								'value': 0,
								'min': 0,
								'type': 'number'
							}
						};

						let inpgroup = {
							'type': 'InputGroup',
							'namespace': 'dash_bootstrap_components',
							'props': {
								'children': [inpgtext, inpval]
							}
						};

						children.push(inpgroup);
					}

					return children;
				}
				else if (gen_value == 'Random Generator') {
					let inpgtext = {
						'type': 'InputGroupText',
						'namespace': 'dash_bootstrap_components',
						'props': {
							'children': 'Number of jobs',
							'style': {
								'width': '50%'
							}
						}
					};

					let inpval = {
						'type': 'Input',
						'namespace': 'dash_bootstrap_components',
						'props': {
							'value': 1,
							'min': 1,
							'type': 'number'
						}
					};

					let inpgroup = {
						'type': 'InputGroup',
						'namespace': 'dash_bootstrap_components',
						'props': {
							'children': [inpgtext, inpval]
						}
					};

					return inpgroup;
				}
				else return window.dash_clientside.no_update;
			}
			else if (triggered_id == 'workloads-machines-select') {
				if (gen_value == 'Dictionary Generator') {
					let children = [];

					for (workload of data[machine][suite]) {
						let inpgtext = {
							'type': 'InputGroupText',
							'namespace': 'dash_bootstrap_components',
							'props': {
								'children': workload,
								'style': {
									'width': '50%'
								}
							}
						};

						let inpval = {
							'type': 'Input',
							'namespace': 'dash_bootstrap_components',
							'props': {
								'value': 0,
								'min': 0,
								'type': 'number'
							}
						};

						let inpgroup = {
							'type': 'InputGroup',
							'namespace': 'dash_bootstrap_components',
							'props': {
								'children': [inpgtext, inpval]
							}
						};

						children.push(inpgroup);
					}

					return children;

				}
				else return window.dash_clientside.no_update;
			}
			else if (triggered_id == 'workloads-suites-select') {
				if (gen_value == 'Dictionary Generator') {
					let children = [];

					for (workload of data[machine][suite]) {
						let inpgtext = {
							'type': 'InputGroupText',
							'namespace': 'dash_bootstrap_components',
							'props': {
								'children': workload,
								'style': {
									'width': '50%'
								}
							}
						};

						let inpval = {
							'type': 'Input',
							'namespace': 'dash_bootstrap_components',
							'props': {
								'value': 0,
								'min': 0,
								'type': 'number'
							}
						};

						let inpgroup = {
							'type': 'InputGroup',
							'namespace': 'dash_bootstrap_components',
							'props': {
								'children': [inpgtext, inpval]
							}
						};

						children.push(inpgroup);
					}

					return children;

				}
				else return window.dash_clientside.no_update;
			}
			else return window.dash_clientside.no_update;
			
		}
	}
})
