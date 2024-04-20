#include "Job.hpp"
#include <string>
#include <unordered_map>
#include <vector>
#include <algorithm>
#include <numeric>
#include <cmath>


Job::Job() {
	this->job_id = -1;
	this->job_name = std::string("");
	this->num_of_processes = -1L;
	this->binded_cores = -1L;
	this->remaining_time = -1.0;
	this->arrival_time = -1.0;
	this->waiting_time = -1.0;
	this->wall_time = -1.0;
	this->current_speedup = 1.0;
	this->max_coscheduled_speedup = 1.0;
	this->overall_coscheduled_speedup = 1.0;
}

Job::Job(
	std::unordered_map<std::string, double>& speedups_map,
	int job_id,
	std::string job_name,
	long num_of_processes, 
	long binded_cores,
	double remaining_time, 
	double arrival_time, 
	double waiting_time, 
	double wall_time
) {
	this->speedups_map = speedups_map;
	this->job_id = job_id;
	this->job_name = job_name;
	this->num_of_processes = num_of_processes;
	this->remaining_time = remaining_time;
	this->arrival_time = arrival_time;
	this->waiting_time = waiting_time;
	this->wall_time = wall_time;
	this->current_speedup = 1.0;

	// Calculate the max and overall speedups of the job
	std::vector<double> speedups {};
	std::unordered_map<std::string, double>::iterator it {};

	for (it = speedups_map.begin(); it != speedups_map.end(); it++)
		speedups.push_back(it->second);

	if (speedups.size() == 0) {
		this->max_coscheduled_speedup = 1.0;
		this->overall_coscheduled_speedup = 1.0;
	}
	else {
		// Job's max co-scheduled speedup
		this->max_coscheduled_speedup = *std::max_element(speedups.begin(), speedups.end());

		// Job's overall co-scheduled speedup
		this->overall_coscheduled_speedup = std::accumulate(speedups.begin(), speedups.end(), 0.0) / speedups.size();
	}
}

Job::Job(const Job& copy_job) {

	this->job_id = copy_job.job_id;
	this->job_name = copy_job.job_name;

	this->num_of_processes = copy_job.num_of_processes;
	this->full_node_cores = copy_job.full_node_cores;
	this->half_node_cores = copy_job.half_node_cores;

	this->remaining_time = copy_job.remaining_time;
	this->arrival_time = copy_job.arrival_time;
	this->waiting_time = copy_job.waiting_time;
	this->wall_time = copy_job.wall_time;

	this->speedups_map = copy_job.speedups_map;
	this->current_speedup = copy_job.current_speedup;
	this->max_coscheduled_speedup = copy_job.max_coscheduled_speedup;
	this->overall_coscheduled_speedup = copy_job.overall_coscheduled_speedup;
}

bool Job::operator==(const Job & comp_job) {

	bool condition = (this->speedups_map == comp_job.speedups_map);

	condition &= (this->job_id == comp_job.job_id);
	condition &= (this->job_name == comp_job.job_name);
	condition &= (this->num_of_processes == comp_job.num_of_processes);
	condition &= (this->binded_cores == comp_job.binded_cores);
	condition &= (this->full_node_cores == comp_job.full_node_cores);
	condition &= (this->half_node_cores == comp_job.half_node_cores);
	condition &= (this->remaining_time == comp_job.remaining_time);
	condition &= (this->arrival_time == comp_job.arrival_time);
	condition &= (this->waiting_time == comp_job.waiting_time);
	condition &= (this->wall_time == comp_job.wall_time);
	condition &= (this->current_speedup == comp_job.current_speedup);
	condition &= (this->max_coscheduled_speedup == comp_job.max_coscheduled_speedup);
	condition &= (this->overall_coscheduled_speedup == comp_job.overall_coscheduled_speedup);

	return condition;
}

double Job::get_speedup(const Job & co_job) {
	return this->speedups_map[co_job.job_name];
}

double Job::get_max_speedup() {
	return this->max_coscheduled_speedup;
}

double Job::get_overall_speedup() {
	return this->overall_coscheduled_speedup;
}

void Job::ratioed_remaining_time(const Job & co_job) {

	double old_speedup = this->current_speedup;
	double new_speedup = this->get_speedup(co_job);

	this->remaining_time *= (old_speedup / new_speedup);

	this->current_speedup = new_speedup;

}

void Job::set_full_node_cores(long cores_per_node) {
	this->full_node_cores = long( std::ceil(this->num_of_processes / cores_per_node) * cores_per_node );
}

void Job::set_half_node_cores(long cores_per_node) {
	this->half_node_cores = long( std::ceil(this->num_of_processes / (cores_per_node / 2.0)) * (cores_per_node / 2.0) );
}
