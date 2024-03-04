import pytest
import sys
import os


# API
sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../../../../"
)))

# REALSIM
sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../../../"
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

print("Testing free_resources implementation")

def inject_code(self):
    """Check if the free cores of the cluster never fall down from zero
    or surpass the total number of cores of the cluster
    """

    res_start = self.free_cores >= 0 and self.free_cores <= self.total_cores
    self.free_resources_base()
    res_end = self.free_cores >= 0 and self.free_cores <= self.total_cores

    self.test_result &= res_start and res_end

ClusterExhaustive.free_resources_base = ClusterExhaustive.free_resources
ClusterExhaustive.free_resources = inject_code

ClusterShallow.free_resources_base = ClusterShallow.free_resources
ClusterShallow.free_resources = inject_code

fixture_params = [(nums, schema )
                  for nums in [20, 30, 50]
                  for schema in ["exhaustive", "shallow", "compact"]]

@pytest.fixture(params=fixture_params, ids=[f"jobs = {param[0]}, cluster schema = {param[1]}" for param in fixture_params])
def setup_sim(request):

    lm = LoadManager("aris.compute", "NAS")
    lm.import_from_db(username="admin", password="admin", dbname="storehouse")
    gen = RandomGenerator(lm)
    jobs_set = gen.generate_jobs_set(request.param[0])

    if request.param[1] == "exhaustive":
        # Change next_state in ClusterExhaustive
        cluster = ClusterExhaustive(426, 20)
        scheduler = BalancerFullOn()
    elif request.param[1] == "shallow":
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

    return cluster

def test_free_resources(setup_sim):
    setup_sim.run()
    assert setup_sim.test_result
