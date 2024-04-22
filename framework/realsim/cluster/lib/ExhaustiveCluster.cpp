#include "ExhaustiveCluster.hpp"
#include <stdexcept>
#include <limits>
#include <algorithm>


ExhaustiveCluster::ExhaustiveCluster(unsigned long nodes, unsigned long cores_per_node)
: AbstractCluster(nodes, cores_per_node) {}


void ExhaustiveCluster::next_state() {

	// The minimum time for the next step of the simulation
	double min_step_time = std::numeric_limits<double>::infinity();

	// Set the minimum step time as the smallest remaining time for an executing job
	// if it is less than the current value
	for (std::vector<Job>& jobs_block : this->execution_list) {
		for (Job& job : jobs_block) {
			if ((typeid(&job) != typeid(EmptyJob)) && (job.remaining_time < min_step_time))
				min_step_time = job.remaining_time;
		}
	}

	// Set the minimum step time as the smallest time for the next arrival of a job
	// if it is less than the current value
	double showup_time {};
	for (Job& job : this->preload_jobs) {
		showup_time = job.arrival_time - this->makespan;
		if ((showup_time > 0) && (showup_time < min_step_time))
			min_step_time = showup_time;
	}

	if (!(min_step_time < std::numeric_limits<double>::infinity()))
		throw std::runtime_error("<next_state> couldn't find a minimal next step time");

	// Given the minimum next step time we increase the makespan of the cluster
	// and the waiting time of the job by its value
	this->makespan += min_step_time;

	for (Job& job : this->waiting_queue)
		job.waiting_time += min_step_time;

	// Create a new execution list for the new state of the cluster
	std::vector<std::vector<Job>> execution_list {};

	for (std::vector<Job>& jobs_block : this->execution_list) {
		// Construct a new jobs_block that has all the still executing jobs at
		// the beginning and the finished jobs (EmptyJob) at the end
		std::vector<Job> substitute_jobs_block {};
		std::vector<EmptyJob> empty_jobs {};
		unsigned long max_binded_cores {};

		for (Job& job : jobs_block) {

			// If job has already finished then put it with the other
			// finished jobs
			if (typeid(job) == typeid(EmptyJob))
				empty_jobs.push_back(job);
			else {
				// Else calculate the remaining time for execution
				job.remaining_time -= min_step_time;

				// If there was a miscalculation in the simulation throw a runtime error
				if (job.remaining_time < 0.0)
					throw std::runtime_error("Miscalculated job's remaining time less than zero");

				// If remaining time hits zero then put it with the other finished jobs
				if (job.remaining_time == 0.0)
					empty_jobs.push_back(EmptyJob(job));
				else {
					// Else if the binded cores of the job are the largest in the vector set it as first
					if (job.binded_cores > max_binded_cores) {
						substitute_jobs_block.emplace(substitute_jobs_block.begin(), job);
						max_binded_cores = job.binded_cores;
					}
					// Otherwise put it with the other executing jobs at the end
					else substitute_jobs_block.push_back(job);
				}
			}

		}

		// If the substitute block of jobs is not empty then recalculate their speedup
		if (substitute_jobs_block.size() > 1) {

			// Calculate the speedup for the jobs at the tail
			std::vector<Job>::iterator largest_job_it {substitute_jobs_block.begin()};
			std::vector<Job>::iterator it {substitute_jobs_block.begin() + 1};
			while (it != substitute_jobs_block.end()) {
				if (it->current_speedup != it->get_speedup(*largest_job_it))
					it->ratioed_remaining_time(*largest_job_it);
			}

			// Calculate the speedup for the largest job by finding the worst speedup
			// combination
			std::vector<Job>::iterator worst_job = std::min_element(
					largest_job_it+1, 
					substitute_jobs_block.end(), 
					[&](Job co_job) {return largest_job_it->get_speedup(co_job)}
				);

			if (largest_job_it->current_speedup != largest_job_it->get_speedup(*worst_job))
				largest_job_it->ratioed_remaining_time(*worst_job);
			
		}

	}

}

void ExhaustiveCluster::free_resources() {

}
