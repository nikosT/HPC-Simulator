# cython: c_string_type=unicode, c_string_encoding=utf8

from libcpp.algorithm cimport shuffle
from libcpp.random cimport default_random_engine
from libcpp.chrono cimport system_clock
from cython.operator cimport dereference as deref
cimport dictgen


cdef class DictionaryGenerator(AbstractGenerator):

    def __init__(self, object load_manager):
        self.name = "Dictionary Generator"
        self.description = "Generate jobs by setting their frequency inside the set"
        AbstractGenerator.__init__(self, load_manager=load_manager)

    cdef vector[Job] generate_jobs_set(self, unordered_map[string, int] arg):

        # Important variables to create a job
        cdef int idx = 0
        cdef vector[Job] jobs_set

        # Iterate through map
        cdef string name
        cdef int frequency
        cdef unordered_map[string, int].iterator it = arg.begin()

        # for load_name, freq in arg.items():
        while it != arg.end():

            name = <string>(it.first)
            frequency = <int>(it.second)

            for _ in range(frequency):
                jobs_set.push_back( <Job>self.generate_job(idx, load) )
                idx += 1

        unsigned int seed = system_clock.now().time_since_epoch().count()
        default_random_engine engine(seed)
        shuffle(jobs_set.begin(), jobs_set.end(), engine)

        return jobs_set
