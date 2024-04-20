#ifndef JOB_HPP
#define JOB_HPP

#include <unordered_map>
#include <string>

class Job {

	public:
		// Job's identifiers
		int job_id;
		std::string job_name;

		// Cores specific values
		long num_of_processes;
		long binded_cores;
		long full_node_cores;
		long half_node_cores;

		// Job's timers
		double remaining_time;
		double arrival_time;
		double waiting_time;
		double wall_time;

		// Job's speedups for each co-job
		std::unordered_map<std::string, double> speedups_map;
		
		// Speedup values
		double current_speedup;
		double max_coscheduled_speedup;
		double overall_coscheduled_speedup;


		// Constructors
		Job();
		Job(
			std::unordered_map<std::string, double>&,
			int,
			std::string,
			long, long,
			double, double, double, double
		);
		Job(const Job&);

		// Methods
		bool operator==(const Job&);
		double get_speedup(const Job&);
		double get_max_speedup();
		double get_overall_speedup();
		void ratioed_remaining_time(const Job&);
		void set_full_node_cores(long);
		void set_half_node_cores(long);

};

class EmptyJob : public Job {
	public:
		EmptyJob(const Job&);
};


#endif
