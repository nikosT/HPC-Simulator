# cython: c_string_type=unicode, c_string_encoding=utf8

# Python imports
from numpy.random import seed, randint, random_sample
from time import time_ns

from libcpp.string cimport string
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

        cdef int idx
        cdef vector[Job] jobs_set
        for i, load_name in enumerate(names):
            idx = <int>i
            jobs_set.push_back( <Job>self.generate_job(idx, <string>load_name) )

        return jobs_set
