# Cython imports
from libcpp.vector cimport vector
from abstractgen cimport AbstractGenerator
from job cimport Job


cdef class RandomGenerator(AbstractGenerator):
    cdef vector[Job] generate_jobs_set(self, int arg)
