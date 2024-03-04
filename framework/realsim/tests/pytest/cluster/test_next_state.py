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

ClusterExhaustive.test_result = True
ClusterShallow.test_result = True
    
print("Testing next_state implementation correctness")

def inject_code_f1(self):
    res_start =  self.free_cores >= 0 and self.free_cores <= self.total_cores
    self.next_state_base()
    res_end = self.free_cores >= 0 and self.free_cores <= self.total_cores
    self.test_result &= res_start and res_end

def inject_code_f2(self):

    exec_list_copy = deepcopy_list(self.execution_list)

    min_time = math.inf
    for unit in self.execution_list:
        for job in unit:
            if type(job) != EmptyJob and job.remaining_time < min_time:
                min_time = job.remaining_time

    self.next_state_base()

    result = True

    executing_jobs = list()
    empty_jobs = list()
    for unit in self.execution_list:
        for job in unit:
            if type(job) == EmptyJob and job.job_id >= 0:
                empty_jobs.append(job)
            if type(job) != EmptyJob:
                executing_jobs.append(job)

    # Test executing jobs
    for job in executing_jobs:

        for past_unit in exec_list_copy:

            for past_job in past_unit:

                if past_job.job_id == job.job_id:

                    job_time = job.remaining_time

                    if job.speedup != past_job.speedup:
                        job_time *= (job.speedup / past_job.speedup)

                    job_time += min_time

                    lesser = (1.01 * past_job.remaining_time >= job_time)
                    greater = (0.99 * past_job.remaining_time <= job_time)
                    together = lesser and greater
                    result &= together

                    if result == False:
                        print("ERROR:", past_job, job, min_time)
                        break

            if result == False:
                break

        if result == False:
            break

    if result == False:
        assert False

    # Test empty jobs
    for job in empty_jobs:
        for past_unit in exec_list_copy:
            for past_job in past_unit:
                if job.job_id == past_job.job_id and type(past_job) != EmptyJob:
                    result &= (past_job.remaining_time == min_time)
                    if result == False:
                        print("ERROR:", past_job, job, min_time)

            if result == False:
                break

        if result == False:
            break

    self.test_result &= result

def inject_code_f3(self):

    exec_list_copy = deepcopy_list(self.execution_list)

    min_time = math.inf
    for unit in self.execution_list:
        for job in unit:
            if type(job) != EmptyJob and job.remaining_time < min_time:
                min_time = job.remaining_time

    self.next_state_base()

    new_emptyjobs = list()
    for unit in self.execution_list:
        for job in unit:
            if type(job) == EmptyJob:
                new_emptyjobs.append(job)

    result = True
    for job in new_emptyjobs:
        for past_unit in exec_list_copy:
            for past_job in past_unit:
                if past_job.job_id == job.job_id and type(past_job) != EmptyJob:
                    result &= past_job.remaining_time == min_time
                    if result == False:
                        print("ERROR:", past_job, job, min_time)
                        break

            if result == False:
                break

        if result == False:
            break

    self.test_result &= result

# Define cluster classes for testing different features
class ClusterExhaustiveFix1(ClusterExhaustive):
    pass
class ClusterExhaustiveFix2(ClusterExhaustive):
    pass
class ClusterExhaustiveFix3(ClusterExhaustive):
    pass

class ClusterShallowFix1(ClusterShallow):
    pass
class ClusterShallowFix2(ClusterShallow):
    pass
class ClusterShallowFix3(ClusterShallow):
    pass

ClusterExhaustiveFix1.next_state_base = ClusterExhaustiveFix1.next_state
ClusterExhaustiveFix1.next_state = inject_code_f1

ClusterExhaustiveFix2.next_state_base = ClusterExhaustiveFix2.next_state
ClusterExhaustiveFix2.next_state = inject_code_f2

ClusterExhaustiveFix3.next_state_base = ClusterExhaustiveFix3.next_state
ClusterExhaustiveFix3.next_state = inject_code_f3

ClusterShallowFix1.next_state_base = ClusterShallowFix1.next_state
ClusterShallowFix1.next_state = inject_code_f1

ClusterShallowFix2.next_state_base = ClusterShallowFix2.next_state
ClusterShallowFix2.next_state = inject_code_f2

ClusterShallowFix3.next_state_base = ClusterShallowFix3.next_state
ClusterShallowFix3.next_state = inject_code_f3


refClusterExhaustive = ClusterExhaustive
refClusterShallow = ClusterShallow

@pytest.fixture
def change_class_refs(class_ref):

    global refClusterExhaustive
    global refClusterShallow

    if class_ref == "free_cores":
        refClusterExhaustive = ClusterExhaustiveFix1
        refClusterShallow = ClusterShallowFix1
        # print("\n\tTesting if the implementation doesn't distort the available free cores")
    elif class_ref == "remaining_time":
        refClusterExhaustive = ClusterExhaustiveFix2
        refClusterShallow = ClusterShallowFix2
        # print("\n\tTesting if the implementation subtracts correctly the minimum time")
    else:
        refClusterExhaustive = ClusterExhaustiveFix3
        refClusterShallow = ClusterShallowFix3
        # print("\n\tTesting if the implementation creates the correct EmptyJob instances")

fixture_params = [(nums, schema )
                  for nums in [20, 30, 50]
                  for schema in ["exhaustive", "shallow", "compact"]]

@pytest.fixture(params=fixture_params, ids=[f"jobs = {param[0]}, cluster schema = {param[1]}" for param in fixture_params])
def fixture(request):

    # print(f"\tNum of jobs: {request.param[0]}, Cluster: {request.param[1]}\t", end="")

    lm = LoadManager("aris.compute", "NAS")
    lm.import_from_db(username="admin", password="admin", dbname="storehouse")
    gen = RandomGenerator(lm)
    jobs_set = gen.generate_jobs_set(request.param[0])

    if request.param[1] == "exhaustive":
        # Change next_state in ClusterExhaustive
        cluster = refClusterExhaustive(426, 20)
        scheduler = BalancerFullOn()
    elif request.param[1] == "shallow":
        cluster = refClusterShallow(426, 20)
        scheduler = Balancer()
    else:
        cluster = refClusterExhaustive(426, 20)
        scheduler = CompactScheduler()

    cluster.test_result = True

    cluster.deploy_to_waiting_queue(jobs_set)

    logger = Logger()

    cluster.assign_scheduler(scheduler)
    scheduler.assign_cluster(cluster)

    cluster.assign_logger(logger)
    scheduler.assign_logger(logger)

    return cluster

@pytest.mark.parametrize("class_ref", ["free_cores", "remaining_time", "emptyjobs"])
def test_next_state(change_class_refs, fixture):
    fixture.run()
    assert fixture.test_result

