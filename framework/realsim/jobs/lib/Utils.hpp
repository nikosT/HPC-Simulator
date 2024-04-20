#ifndef UTILS_HPP
#define UTILS_HPP

#include <vector>
#include "Job.hpp"

typedef std::vector<Job> JobsBlock;

std::vector<Job> deepcopy_list(std::vector<Job>&);
std::vector<JobsBlock> deepcopy_list(std::vector<JobsBlock> &);

#endif
