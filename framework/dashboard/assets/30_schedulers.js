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
	}

})
