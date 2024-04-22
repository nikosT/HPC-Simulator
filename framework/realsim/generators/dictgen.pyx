# cython: c_string_type=unicode, c_string_encoding=utf8

from random import shuffle, seed
from time import time_ns

from cython.operator cimport dereference as deref, postincrement as pincr
cimport dictgen


cdef class DictionaryGenerator(AbstractGenerator):

    def __init__(self, object load_manager):
        self.name = "Dictionary Generator"
        self.description = "Generate jobs by setting their frequency inside the set"
        AbstractGenerator.__init__(self, load_manager=load_manager)

    cdef vector[Job] generate_jobs_set(self, unordered_map[string, int]& arg):

        # Important variables to create a job
        cdef int idx = 0
        cdef vector[Job] jobs_set

        # Iterate through map
        cdef string load_name
        cdef int frequency
        cdef unordered_map[string, int].iterator it = arg.begin()

        # for load_name, freq in arg.items():
        while it != arg.end():

            load_name = deref(it).first
            frequency = deref(it).second

            for _ in range(frequency):
                jobs_set.push_back( <Job>self.generate_job(idx, load_name) )
                idx += 1

            pincr(it)

        # Shuffle the resulting set of jobs
        ints = [i for i in range( <object>jobs_set.size() )]
        seed(time_ns() % (2 ** 32))
        shuffle(ints)

        cdef vector[Job] shuffled_jobs_set
        for i in ints:
            shuffled_jobs_set.push_back( jobs_set[<int>i] )

        return shuffled_jobs_set
