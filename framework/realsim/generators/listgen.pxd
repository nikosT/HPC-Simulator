# Cython imports
from libcpp.vector cimport vector
from libcpp.string cimport string
from abstractgen cimport AbstractGenerator
from job cimport Job


cdef class ListGenerator(AbstractGenerator):
    cdef vector[Job] generate_jobs_set(self, vector[string]& arg)
