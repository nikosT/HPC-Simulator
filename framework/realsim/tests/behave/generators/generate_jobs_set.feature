Feature: a generator instance produces a set of jobs from a load manager

	Scenario Outline: Random generator
		Given a random generator with <jobs> jobs
		When we ask for a jobs set
		Then we get a true random set of jobs
	
		Examples: Number of jobs
			| jobs 	|
			| 10 	|
			| 40 	|
			| 100 	|
			| 200 	|
	
	Scenario: Dictionary generator
		Given a dictionary generator
			| load 		| frequency 	|
			| sp.D.121 	| 5 		|
			| bt.E.1024 	| 10 		|
			| cg.E.512 	| 7 		|
		When we ask for a jobs set
		Then we get a random jobs set with the correct loads and their frequencies
	
	Scenario: List generator
		Given a list generator
			| load 		|
			| sp.D.121 	|
			| sp.D.121 	|
			| bt.D.256 	|
			| ft.E.512 	|
			| ft.E.512 	|
			| ep.E.512 	|
		When we ask for a jobs set
		Then we get a jobs set with the same loads and their placement as defined by the list
