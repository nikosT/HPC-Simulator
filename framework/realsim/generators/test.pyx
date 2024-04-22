import os
import sys
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '../../../')))
from api.loader import Load, LoadManager

lm = LoadManager("aris.compute", "NAS")
lm.import_from_db(host="mongodb+srv://cslab:bQt5TU6zQsu_LZ@storehouse.om2d9c0.mongodb.net",
                  dbname="storehouse")

from cython.operator cimport dereference as deref
from libcpp.vector cimport vector
from randomgen cimport RandomGenerator
from job cimport Job

cdef RandomGenerator gen = RandomGenerator(lm)
cdef vector[Job] res = gen.generate_jobs_set(10)
cdef vector[Job].iterator it = res.begin()

while it != res.end():
    print(deref(it).speedups_map)
    it += 1
