# Python imports
from numpy.random import seed, randint, random_sample
from time import time_ns

cimport randomgen

cdef class RandomGenerator(AbstractGenerator):

    def __init__(self, object load_manager):
        self.name = "Random Generator"
        self.description = "Generating random set of jobs from a specific LoadManager instance"
        AbstractGenerator.__init__(self, load_manager=load_manager)

    cdef vector[Job] generate_jobs_set(self, int arg):

        # Get the load names of the load_manager
        keys = list(self.load_manager.loads.keys())

        # Generate random positive integers that will be used as
        # indices to query the loads' names
        seed(time_ns() % (2 ** 32))
        ints = randint(low=0, high=len(keys), size=(arg,))

        # Get the names of the loads
        names = list(map(lambda i: keys[i], ints))
        # Get the loads
        loads = list(map(lambda name: self.load_manager(name), names))

        cdef int idx
        cdef vector[Job] jobs_set
        for i, load in enumerate(loads):
            idx = <int>i
            jobs_set.push_back( <Job>self.generate_job(idx, load) )

        return jobs_set
