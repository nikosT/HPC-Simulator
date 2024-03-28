window.dash_clientside = Object.assign({}, window.dash_clientside, {
	clientside: {
		create_schedulers_checklist: function(data) {
			options = [];
			selected = [];
			for (i = 0; i < data.length; i++) {
				item = {
					'label': data[i].name,
					'value': i,
					'disabled': data[i].disabled,
				};

				options.push(item);

				if (data[i].selected) {
					selected.push(i)
				}
			}

			child = {
				'type': 'Checklist',
				'namespace': 'dash_bootstrap_components',
				'props': {
					'id': 'selected-schedulers',
					'options': options,
					'value': selected,
				}
			};

			return child;
		}
	}
})
