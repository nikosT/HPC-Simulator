# cython: c_string_type=unicode, c_string_encoding=utf8

from cython.operator cimport dereference as deref

cdef class ListGenerator(AbstractGenerator):

    def __init__(self, object load_manager):
        self.name = "List Generator"
        self.description = "Generate jobs based on the list of names given by the user"
        AbstractGenerator.__init__(self, load_manager=load_manager)

    cdef vector[Job] generate_jobs_set(self, vector[string]& arg):

        cdef vector[Job] jobs_set

        cdef int idx = 0
        cdef string load_name
        cdef vector[string].iterator it = arg.begin()

        while it != arg.end():
            load_name = deref(it)
            jobs_set.push_back( <Job>self.generate_job(idx, load_name) )
            idx += 1
            it += 1

