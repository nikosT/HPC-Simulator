from libcpp.string cimport string
from job cimport Job

cdef class AbstractGenerator:
    cdef string name
    cdef string description
    cdef object load_manager
    cdef Job generate_job(self, int idx, load: Load)
