# cython: c_string_type=unicode, c_string_encoding=utf8

# Python libraries
import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))
from api.loader import Load, LoadManager
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../')))

# Cython libraries
from libcpp.unordered_map cimport unordered_map
cimport abstractgen


cdef class AbstractGenerator:

    def __init__(self, object load_manager):
        self.name = "Abstract Generator"
        self.description = "Abstract base class for all generators"
        self.load_manager = load_manager

    cdef Job generate_job(self, int idx, load: Load):

        cdef string name
        cdef double speedup
        cdef unordered_map[string, double] speedups_map

        for coload_name in load.coloads:
            
            name = <string>coload_name
            speedup = <double>(self.load_manager(load.full_load_name).get_median_speedup( coload_name ))

            speedups_map[name] = speedup

        return Job(speedups_map, 
                   idx, 
                   <string>load.full_load_name, 
                   <long>load.num_of_processes, 
                   <long>load.num_of_processes, 
                   <double>load.get_median(),
                   0.0,
                   0.0,
                   0.0)
