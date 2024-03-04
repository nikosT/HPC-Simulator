import behave
import sys
import os


# API
sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../../../../../../../"
)))

# REALSIM
sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../../../../../../"
)))

import math
from api.loader import LoadManager
from realsim.jobs.jobs import EmptyJob
from realsim.jobs.utils import deepcopy_list
from realsim.generators.random import RandomGenerator
from realsim.cluster.shallow import ClusterShallow
from realsim.cluster.exhaustive import ClusterExhaustive
from realsim.scheduler.balancerFullOn import BalancerFullOn
from realsim.scheduler.balancer import Balancer
from realsim.scheduler.compact import CompactScheduler
from realsim.logger.logger import Logger

# Setup clusters
ClusterExhaustive.free_resources_base = ClusterExhaustive.free_resources
ClusterShallow.free_resources_base = ClusterShallow.free_resources

def free_resources_free_cores_bounds(self):
    """Free cores bounded for each call
    """

    res_start =  self.free_cores >= 0 and self.free_cores <= self.total_cores
    self.free_resources_base()
    res_end = self.free_cores >= 0 and self.free_cores <= self.total_cores

    self.test_result &= res_start and res_end

def free_resources_free_cores_addup(self):
    """Free cores add up after each call
    """

    # Starting free cores of cluster before call of free_resources
    start_free_cores = self.free_cores

    # Cores that will be returned to cluster
    to_return_cores = 0

    for unit in self.execution_list:
        empty = True
        for job in unit:
            if type(job) != EmptyJob:
                empty = False
                break

        # If the unit is empty
        if empty:
            # If the unit is compact
            if len(unit) == 1:
                to_return_cores += unit[0].binded_cores
            elif len(unit) > 1:
                to_return_cores += 2 * unit[0].binded_cores
            else:
                raise RuntimeError("There is a unit with no job inside")

    # Call free_resources
    self.free_resources_base()

    # Test if the free cores addup with their previous value
    self.test_result &= (self.free_cores == start_free_cores + to_return_cores)

def free_resources_removed_emptyjobs(self):
    """Removed finished execution units
    """

    exec_list_copy = deepcopy_list(self.execution_list)
    
    for unit in self.execution_list:
        empty = True
        for job in unit:
            if type(job) != EmptyJob:
                empty = False
                break

        if empty:
            # Remove from copy
            exec_list_copy.remove(unit)

    # Call free_resources
    self.free_resources_base()

    # Check if the lists are the same (we don't care about position)
    for unit in self.execution_list:
        self.test_result &= (unit in exec_list_copy)
        if self.test_result == False:
            print("ERROR: the following execution unit wasn't removed", unit)
            break

ClusterExhaustive.free_resources_free_cores_bounds = free_resources_free_cores_bounds
ClusterExhaustive.free_resources_free_cores_addup = free_resources_free_cores_addup
ClusterExhaustive.free_resources_removed_emptyjobs = free_resources_removed_emptyjobs

ClusterShallow.free_resources_free_cores_bounds = free_resources_free_cores_bounds
ClusterShallow.free_resources_free_cores_addup = free_resources_free_cores_addup
ClusterShallow.free_resources_removed_emptyjobs = free_resources_removed_emptyjobs


@behave.given("{jobs} of random jobs of {machine}, a(n) {schema} cluster, a scheduler and a logger")
def given_impl(context, jobs, machine, schema):

    lm = LoadManager(machine, "NAS")
    lm.import_from_db(username="admin", password="admin", dbname="storehouse")
    gen = RandomGenerator(lm)
    jobs_set = gen.generate_jobs_set(int(jobs))

    if schema == "exhaustive":
        cluster = ClusterExhaustive(426, 20)
        scheduler = BalancerFullOn()
    elif schema == "shallow":
        cluster = ClusterShallow(426, 20)
        scheduler = Balancer()
    else:
        cluster = ClusterExhaustive(426, 20)
        scheduler = CompactScheduler()

    cluster.test_result = True

    cluster.deploy_to_waiting_queue(jobs_set)

    logger = Logger()

    cluster.assign_scheduler(scheduler)
    scheduler.assign_cluster(cluster)

    cluster.assign_logger(logger)
    scheduler.assign_logger(logger)

    context.cluster = cluster

@behave.when("we run the simulation")
def when_impl(context):

    if "Free cores bounds" in context.scenario.name:
        context.cluster.free_resources = context.cluster.free_resources_free_cores_bounds

    elif "Free cores add up after call" in context.scenario.name:
        context.cluster.free_resources = context.cluster.free_resources_free_cores_addup

    elif "Removed finished execution units" in context.scenario.name:
        context.cluster.free_resources = context.cluster.free_resources_removed_emptyjobs

    else:
        raise RuntimeError("Couldn't find scenario")

    # Setup cluster's test result
    context.cluster.test_result = True
    context.cluster.run()

@behave.then("for every call to free_resources the number of free cores is greater or equal to zero and lesser or equal to the total number of cores")
def then_free_cores_bounds(context):
    assert context.cluster.test_result

@behave.then("after every call to free_resources the number of free cores adds up correctly")
def then_free_cores_addup(context):
    assert context.cluster.test_result

@behave.then("after every call to free_resources the correct empty units are removed from the execution list")
def then_emptyjobs(context):
    assert context.cluster.test_result
