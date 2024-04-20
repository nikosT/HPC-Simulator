from libcpp.vector cimport vector
from job cimport Job


cdef extern from "lib/Utils.cpp":
    pass

cdef extern from "lib/Utils.hpp":
    cdef vector[Job] deepcopy_list(vector[Job])
    cdef vector[vector[Job]] deepcopy_list(vector[vector[Job]])
