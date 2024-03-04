Feature: Adding any number of load managers is a mathematical closure.

	When we add any number of load managers we want as a result a load
	manager that aggregates all the loads of each manager.

	Scenario: Adding two load managers of the same machine and different suite each
		Given two load managers that manage loads of different suites on the same machine
		When we add the two
		Then we get a new load manager with all the loads of both managers on the same machine
	
	Scenario: Adding two load manager of different machines irrelevant of suite
		Given two load managers which have loads of different machines
		When we add the two
		Then we get an empty load manager
