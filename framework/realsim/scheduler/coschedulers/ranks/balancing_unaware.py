import os
import sys
from typing import Optional

sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../../../../"
)))

from realsim.jobs import Job, EmptyJob
from .balancing import BalancingRanksCoscheduler

from numpy import average as avg
from numpy.random import seed, randint
from time import time_ns
import math


class BalancingRanksCoschedulerXUnaware(BalancingRanksCoscheduler):

    name = "Execution Unaware Co-Scheduler"

    description = """Balancing is the method to achieve at least 1 overall
    speedup for the execution list that is deployed by the scheduling algorithm.
    The main metric ll_avg_speedup calculates the average speedup of the
    execution list as it is being constructed from the lefthand side. The
    specific implementation also has a metric in order to reduce the vertical 
    fragmentation of the execution list. The specific iteration of the balancing
    methodology uses the `ranks` deploying architecture.
    """

    def __init__(self):
        BalancingRanksCoscheduler.__init__(self)
        self.backfill_enabled = True


    def xunit_candidates_reorder(self, job: Job, xunit: list[Job]) -> float:
        seed(time_ns() % (2 ** 32))
        return float(randint(len(self.cluster.execution_list)))
