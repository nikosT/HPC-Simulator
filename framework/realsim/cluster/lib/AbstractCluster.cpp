#include "AbstractCluster.hpp"
#include <algorithm>
#include <typeinfo>

// Default Constructor
AbstractCluster::AbstractCluster() {
	this->nodes = 0;
	this->cores_per_node = 0;
	this->total_cores = 0;
	this->free_cores = 0;

	this->preloaded_queue = std::vector<Job>(0);
	this->waiting_queue = std::vector<Job>(0);
	this->execution_list = std::vector<std::vector<Job>>(0);
	this->finished_jobs = std::vector<int>(0);
	
	this->current_job_id = 0;
	this->makespan = 0.0;
}

// Parametrized Constructor
AbstractCluster::AbstractCluster(unsigned long nodes, unsigned long cores_per_node) {
	this->nodes = nodes;
	this->cores_per_node = cores_per_node;
	this->total_cores = nodes * cores_per_node;
	this->free_cores = this->total_cores;

	this->preloaded_queue = std::vector<Job>(0);
	this->waiting_queue = std::vector<Job>(0);
	this->execution_list = std::vector<std::vector<Job>>(0, std::vector<Job>(0));
	this->finished_jobs = std::vector<int>(0);
	
	this->current_job_id = 0;
	this->makespan = 0.0;
}

void AbstractCluster::setup() {
	this->free_cores = this->total_cores;
	this->makespan = 0.0;
	this->execution_list = std::vector<std::vector<Job>>(0, std::vector<Job>(0));
}

void AbstractCluster::preload_jobs(std::vector<Job>& jobs_set) {
	// Sort the jobs_set by arrival time of each job
	std::sort(jobs_set.begin(), jobs_set.end(), [&](Job job){ return job.arrival_time; });

	if (this->makespan == 0.0)
		this->current_job_id = 0;

	for (Job job : jobs_set) {

		job.job_id = current_job_id;
		job.full_node_cores = job.set_full_node_cores(this->cores_per_node);
		job.half_node_cores = job.set_half_node_cores(this->cores_per_node);
		this->preloaded_queue.push_back(job);

		this->current_job_id++;
	}
}

void AbstractCluster::load_in_waiting_queue() {

	std::vector<Job> new_preloaded_queue {};

	for (Job job : this->preloaded_queue) {
		if (job.arrival_time <= this->makespan)
			this->waiting_queue.push_back(job);
		else
			new_preloaded_queue.push_back(job);
	}

	this->preloaded_queue = new_preloaded_queue;
}

std::vector<std::vector<Job>*> 
AbstractCluster::filled_xunits() {

	std::vector<std::vector<Job>*> filled_units {};

	for (std::vector<Job>& jobs_block : this->execution_list) {

		bool filled = true;

		for (Job& job : jobs_block) {
			if (typeid(&job) == typeid(EmptyJob)) {
				filled = false;
				break;
			}
		}

		if (filled) {
			std::vector<Job> * ptr = &jobs_block;
			filled_units.push_back(ptr);
		}

	}

	return filled_units;

}

std::vector<std::vector<Job>*>
AbstractCluster::nonfilled_xunits() {

	std::vector<std::vector<Job>*> nonfilled_units {};

	for (std::vector<Job>& jobs_block : this->execution_list) {

		bool filled = true;

		for (Job& job : jobs_block) {
			if (typeid(&job) == typeid(EmptyJob)) {
				filled = false;
				break;
			}
		}

		if (!filled) {
			std::vector<Job> * ptr = &jobs_block;
			nonfilled_units.push_back(ptr);
		}

	}

	return nonfilled_units;

}

void AbstractCluster::step() {

	this->load_in_waiting_queue();

	while ((!this->preloaded_queue.empty()) || (!this->waiting_queue.empty()) || (!this->execution_list.empty())) {

		if (!this->waiting_queue.empty()) {
			
			// Submit jobs or block of jobs in execution list
			this->scheduler.submit();

			// If the scheduler submitted successfuly a new batch of jobs and 
			// backfilling is enabled then restart the process to search for new jobs
			if (this->scheduler.submitted && this->scheduler.backfill_enabled)
				continue;

		}

		// Execute jobs in the execution list or move to the next arrival of a job
		this->next_state();

		// If there are jobs in the execution list search those that have Finished
		// and free up the resources
		if (!this->execution_list.empty())
			this->free_resources();

	}

}
