Feature: a cluster instance moves to the next deploying state by executing the next_state method
	We test how the method next_state handles the free cores of the cluster,
	how it subtracts the remaining_time of the minimum job of the execution list from the rest of the jobs
	and how it creates the EmptyJob instances.

	Scenario Outline: Free cores
		Given <jobs> of random jobs of <machine>, a(n) <schema> cluster, a scheduler and a logger
		When we run the simulation
		Then for every call to next_state the number of free cores is greater or equal to zero and lesser or equal to the total number of cores

		Examples: Recurring config
			| jobs 	| machine  	| schema 	|
			| 50 	| aris.compute 	| exhaustive 	|
			| 47 	| aris.compute 	| shallow 	|
			| 15 	| marconi 	| exhaustive 	|
			| 50 	| marconi 	| shallow 	|

	Scenario Outline: Remaining time
		Given <jobs> of random jobs of <machine>, a(n) <schema> cluster, a scheduler and a logger
		When we run the simulation
		Then for every call to next_state the remaining time of the minimal job is subtracted correctly

		Examples: Recurring config
			| jobs 	| machine  	| schema 	|
			| 50 	| aris.compute 	| exhaustive 	|
			| 47 	| aris.compute 	| shallow 	|
			| 15 	| marconi 	| exhaustive 	|
			| 50 	| marconi 	| shallow 	|

	Scenario Outline: Empty jobs
		Given <jobs> of random jobs of <machine>, a(n) <schema> cluster, a scheduler and a logger
		When we run the simulation
		Then for every call to next_state the jobs with 0 remaining time are converted to EmptyJob instances

		Examples: Recurring config
			| jobs 	| machine  	| schema 	|
			| 50 	| aris.compute 	| exhaustive 	|
			| 47 	| aris.compute 	| shallow 	|
			| 15 	| marconi 	| exhaustive 	|
			| 50 	| marconi 	| shallow 	|
	
	Scenario Outline: Every first job has the largest amount of binded cores
		Given <jobs> of random jobs of <machine>, a(n) <schema> cluster, a scheduler and a logger
		When we run the simulation
		Then for every call to next_state the first job for each execution unit has the largest number of binded cores

		Examples: Recurring config
			| jobs 	| machine  	| schema 	|
			| 50 	| aris.compute 	| exhaustive 	|
			| 47 	| aris.compute 	| shallow 	|
			| 15 	| marconi 	| exhaustive 	|
			| 50 	| marconi 	| shallow 	|
