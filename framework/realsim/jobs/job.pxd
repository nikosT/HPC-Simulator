from libcpp.string cimport string
from libcpp.unordered_map cimport unordered_map
from libcpp cimport bool

cdef extern from "lib/Job.cpp":
    pass

cdef extern from "lib/Job.hpp":
    cdef cppclass Job:

        int job_id;
        string job_name;

        long num_of_processes;
        long binded_cores;
        long full_node_cores;
        long half_node_cores;

        double remaining_time;
        double arrival_time;
        double waiting_time;
        double wall_time;

        unordered_map[string, double] speedups_map;
        double current_speedup;
        double max_coscheduled_speedup;
        double overall_coscheduled_speedup;


        Job() except +
        Job(unordered_map[string, double], int, string, long, long, double, double, double, double) except +

        bool operator==(Job)
        double get_speedup(Job)
        double get_max_speedup()
        double get_overall_speedup()
        void ratioed_remaining_time(Job)
        void set_full_node_cores()
        void set_half_node_cores()

    cdef cppclass EmptyJob(Job):
        C_EmptyJob(Job)
