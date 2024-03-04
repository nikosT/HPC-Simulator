Feature: Slicing a load manager returns a manager instance that manages the 
	loads defined by the slice if they exist. This feature is a mathematical 
	closure.

	Scenario Outline: Slicing a load manager
		Given a preloaded load manager of <machine> and <suite>
		When we slice it
			| slice					| empty |
			| bt.D.256, sp.E.1024, ft.D.256		|       |
			| cg.D.256, lu.D.256			|       |
			| some random bullshit			|       |
			| 605.lbm_s.1024, 621.miniswp_s.1024 	|       |
		Then we get a load manager that manages the loads defined by the slice if they exist

		Examples: test cases
			| machine 	| suite |
			| aris.compute 	| NAS 	|
			| aris.compute 	| SPEC 	|
			| marconi 	| NAS 	|
			| marconi 	| SPEC 	|
