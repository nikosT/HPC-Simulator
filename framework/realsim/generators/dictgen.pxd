from libcpp.vector cimport vector
from libcpp.unordered_map cimport unordered_map
from libcpp.string cimport string
from abstractgen cimport AbstractGenerator
from job cimport Job


cdef class DictionaryGenerator(AbstractGenerator):
    cdef vector[Job] generate_jobs_set(self, unordered_map[string, int])
