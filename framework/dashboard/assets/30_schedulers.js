Object.assign(window.dash_clientside.clientside, {

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

			// Create info button tooltip that shows the description 
			// of the scheduling algorithm
			let info = {
				'type': 'Button',
				'namespace': 'dash_bootstrap_components',
				'props': {
					'id': data[i].name + ' tooltip',
					'children': [{
						'type': 'I',
						'namespace': 'dash_html_components',
						'props': {
							'className': 'bi bi-info',
							'color': 'info',
						}
					}],
					'color': 'info',
					'outline': true
				}
			};

			let tooltip = {
				'type': 'Tooltip',
				'namespace': 'dash_bootstrap_components',
				'props': {
					'children': data[i].description,
					'target': data[i].name + ' tooltip'
				}
			}

			let sched_inp = {
				'type': 'Row',
				'namespace': 'dash_bootstrap_components',
				'props': {
					'children': [
						{
							'type': 'Col',
							'namespace': 'dash_bootstrap_components',
							'props': {
								'children': [checkbox],
								'width': 11
							}
						},
						{
							'type': 'Col',
							'namespace': 'dash_bootstrap_components',
							'props': {
								'children': [info, tooltip],
								'width': 1
							}
						}
					]
				}
			};

			let collapse_children = [];

			if ('hyperparams' in data[i]) {

				for (let [name, value] of Object.entries(data[i].hyperparams)) {
					let name_inp = {
						'type': 'InputGroupText',
						'namespace': 'dash_bootstrap_components',
						'props': {
							'children': name,
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
								'min': 0,
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
					'is_open': false,
				}
				
			};

			let collapse_inp = {
				'type': 'Row',
				'namespace': 'dash_bootstrap_components',
				'props': {
					'children': [collapse],
					'class_name': 'my-2'
				}
			};

			let input_group = {
				'type': 'Row',
				'namespace': 'dash_bootstrap_components',
				'props': {
					'children': [sched_inp, collapse_inp],
					'class_name': 'element-item m-1 p-2',
					'style': {'width': '100%'}
				}
			};

			children.push(input_group);

		}

		return children;
	},

	show_scheduler_hyperparameters: function(value) {
		return value;
	}

})
