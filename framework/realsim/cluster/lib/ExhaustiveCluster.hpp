#ifndef EXHAUSTIVE_CLUSTER_HPP
#define EXHAUSTIVE_CLUSTER_HPP

#include "AbstractCluster.hpp"

class ExhaustiveCluster : public AbstractCluster {
	public:
		ExhaustiveCluster(unsigned long, unsigned long);
		void next_state();
		void free_resources();
};

#endif
