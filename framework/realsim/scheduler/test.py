import os
import sys
sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), '../../')
))

from realsim.scheduler.coschedulers.ranks.balancing import BalancingRanksCoscheduler


class TestCoscheduler(BalancingRanksCoscheduler):

    name = "Test Co-Scheduler"
    description = ""
