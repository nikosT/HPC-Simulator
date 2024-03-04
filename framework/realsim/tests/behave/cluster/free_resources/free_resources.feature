Feature: a cluster instance flushes the finished jobs with the method free_resources
	We test if the method handles the free cores of a cluster correctly
	and if the finished execution units are removed completely from the execution list

	Scenario Outline: Free cores bounds
		Given <jobs> of random jobs of <machine>, a(n) <schema> cluster, a scheduler and a logger
		When we run the simulation
		Then for every call to free_resources the number of free cores is greater or equal to zero and lesser or equal to the total number of cores

		Examples: Recurring config
			| jobs 	| machine  	| schema 	|
			| 50 	| aris.compute 	| exhaustive 	|
			| 47 	| aris.compute 	| shallow 	|
			| 15 	| marconi 	| exhaustive 	|
			| 50 	| marconi 	| shallow 	|

	Scenario Outline: Free cores add up after call
		Given <jobs> of random jobs of <machine>, a(n) <schema> cluster, a scheduler and a logger
		When we run the simulation
		Then after every call to free_resources the number of free cores adds up correctly

		Examples: Recurring config
			| jobs 	| machine  	| schema 	|
			| 50 	| aris.compute 	| exhaustive 	|
			| 47 	| aris.compute 	| shallow 	|
			| 15 	| marconi 	| exhaustive 	|
			| 50 	| marconi 	| shallow 	|
	
	Scenario Outline: Removed finished execution units
		Given <jobs> of random jobs of <machine>, a(n) <schema> cluster, a scheduler and a logger
		When we run the simulation
		Then after every call to free_resources the correct empty units are removed from the execution list

		Examples: Recurring config
			| jobs 	| machine  	| schema 	|
			| 50 	| aris.compute 	| exhaustive 	|
			| 47 	| aris.compute 	| shallow 	|
			| 15 	| marconi 	| exhaustive 	|
			| 50 	| marconi 	| shallow 	|
