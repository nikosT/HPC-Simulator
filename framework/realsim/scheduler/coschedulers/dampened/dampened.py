from abc import ABC, abstractmethod

import os
import sys

sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../../../../"
)))

from realsim.jobs.jobs import Job
from realsim.scheduler.coscheduler import Coscheduler


class DampenedCoscheduler(Coscheduler, ABC):

    name = "Abstract Dampened Co-Scheduler"

    description = """Base class for co-scheduling algorithms with dampened
    deploying architecture; meaning the number of co-schedules will decrease
    along with the execution of the simulation.
    """

    def __init__(self, threshold: int = 1, engine=None):
        Coscheduler.__init__(self, threshold=threshold, engine=engine)

    def deploying(self):

        # List of jobs to deploy
        deploy_list: list[list[Job]] = list()

        # Co-scheduling waiting jobs with nonfilled executing units
        self.deploying_to_xunits(deploy_list)

        # Co-scheduling between waiting jobs
        self.deploying_wait_pairs(deploy_list)

        # Default (compact) scheduling of waiting jobs
        if deploy_list == []:
            self.deploying_wait_compact(deploy_list)
        
        # If there are jobs to be deployed to the execution list
        # then return True
        if deploy_list != []:
            self.cluster.execution_list.extend(deploy_list)
            # Logger cluster events update
            self.logger.cluster_events["deploying:success"] += 1
            return True

        # If no job is to be deployed then False
        # Logger cluster events update
        self.logger.cluster_events["deploying:failed"] += 1
        return False
