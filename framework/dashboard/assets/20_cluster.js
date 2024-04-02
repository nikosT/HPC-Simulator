Object.assign(window.dash_clientside.clientside, {
	cluster_events: function(n_clicks=0) {

		let visible = {'display': 'block'};
		let invisible = {'display': 'none'};

		if (n_clicks % 2 == 0) {
			// By name configuration
			return ["To manual configuration", visible, invisible]
		}
		else {
			// Manual configuration
			return ["By name configuration", invisible, visible]
		}

	}
})
