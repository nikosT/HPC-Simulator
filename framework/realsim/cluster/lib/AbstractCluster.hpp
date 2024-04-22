#ifndef ABSTRACT_CLUSTER_HPP
#define ABSTRACT_CLUSTER_HPP

#include <vector>

#include "Job.hpp"
#include "Scheduler.hpp"


class AbstractCluster {

	public:
		// Configuration values for clusters
		unsigned long nodes;
		unsigned long cores_per_node;
		unsigned long total_cores;
		unsigned long free_cores;

		// Scheduler, Logger
		Scheduler * scheduler;
		
		// Important queues and lists for cluster
		std::vector<Job> preloaded_queue;
		std::vector<Job> waiting_queue;
		std::vector<std::vector<Job>> execution_list;
		std::vector<int> finished_jobs;

		// Jobs' management
		bool moldability;
		bool malleability;

		// Counters
		int current_job_id;
		double makespan;

		// Constructors
		AbstractCluster();
		AbstractCluster(unsigned long nodes, unsigned long cores_per_node);
		AbstractCluster(const AbstractCluster&);


		// Methods

		void setup();

		void preload_jobs(std::vector<Job>&);
		void load_in_waiting_queue();

		std::vector<std::vector<Job>*> filled_xunits();
		std::vector<std::vector<Job>*> nonfilled_xunits();

		virtual void next_state();
		virtual void free_resources();

		void step();

};

#endif
