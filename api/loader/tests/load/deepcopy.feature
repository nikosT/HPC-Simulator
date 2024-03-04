@load
Feature: Deepcopy of a Load

	Scenario Outline: Check if we get a true deepcopy of a Load instance
		Given a machine: <machine>, a suite: <suite> and a load: <name>
		When we ask for a deepcopy of the load
		Then we get a true deepcopy instance

		Examples: various load managers and loads
			| machine 	| suite | name 		  |
			| aris.compute 	| NAS 	| cg.E.512 	  |
			| aris.compute 	| SPEC 	| 613.soma_s.1024 |
			| aris.compute 	| empty	| 605.lbm_s.1024  |
			| marconi 	| NAS 	| sp.D.256 	  |
			| marconi 	| empty	| bt.D.256 	  |
