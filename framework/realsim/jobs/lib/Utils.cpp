#include "Utils.hpp"

std::vector<Job> deepcopy_list(std::vector<Job>& jobs_list) {

	std::vector<Job> new_list {};
	std::vector<Job>::iterator it {};

	for (it = jobs_list.begin(); it != jobs_list.end(); it++) {
		Job new_job(*it);
		new_list.push_back(new_job);
	}

	return new_list;
}

std::vector<JobsBlock> deepcopy_list(std::vector<JobsBlock>& jobsblock_list) {

	std::vector<JobsBlock> new_jobsblock_list;
	std::vector<JobsBlock>::iterator it {};

	for (it = jobsblock_list.begin(); it != jobsblock_list.end(); it++) {

		std::vector<Job> new_block {};
		std::vector<Job>::iterator jt {};

		for (jt = (*it).begin(); jt != (*it).end(); jt++) {
			Job new_job(*jt);
			new_block.push_back(new_job);
		}

		new_jobsblock_list.push_back(new_block);

	}

	return new_jobsblock_list;

}
