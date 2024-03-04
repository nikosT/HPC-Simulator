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
ClusterExhaustive.next_state_base = ClusterExhaustive.next_state
ClusterShallow.next_state_base = ClusterShallow.next_state

# Setup test functions
def next_state_free_cores(self):

    res_start =  self.free_cores >= 0 and self.free_cores <= self.total_cores
    self.next_state_base()
    res_end = self.free_cores >= 0 and self.free_cores <= self.total_cores

    self.test_result &= res_start and res_end

def next_state_remaining_time(self):

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

def next_state_empty_jobs(self):

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

def next_state_jobs_binded_cores(self):

    # Execute the base next_state method
    self.next_state_base()

    # Check the binded cores of the first job
    for unit in self.execution_list:
        if len(unit) > 1:
            for job in unit[1:]:
                self.test_result &= unit[0].binded_cores >= job.binded_cores

ClusterExhaustive.next_state_free_cores = next_state_free_cores
ClusterExhaustive.next_state_remaining_time = next_state_remaining_time
ClusterExhaustive.next_state_empty_jobs = next_state_empty_jobs
ClusterExhaustive.next_state_jobs_binded_cores = next_state_jobs_binded_cores

ClusterShallow.next_state_free_cores = next_state_free_cores
ClusterShallow.next_state_remaining_time = next_state_remaining_time
ClusterShallow.next_state_empty_jobs = next_state_empty_jobs
ClusterShallow.next_state_jobs_binded_cores = next_state_jobs_binded_cores

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

    if "Free cores" in context.scenario.name:
        context.cluster.next_state = context.cluster.next_state_free_cores
    elif "Remaining time" in context.scenario.name:
        context.cluster.next_state = context.cluster.next_state_remaining_time
    elif "Empty jobs" in context.scenario.name:
        context.cluster.next_state = context.cluster.next_state_empty_jobs
    elif "Every first job has the largest amount of binded cores" in context.scenario.name:
        context.cluster.next_state = context.cluster.next_state_jobs_binded_cores
    else:
        raise RuntimeError("Couldn't find scenario")

    # Setup cluster's test result
    context.cluster.test_result = True
    context.cluster.run()

@behave.then("for every call to next_state the number of free cores is greater or equal to zero and lesser or equal to the total number of cores")
def then_free_cores(context):
    assert context.cluster.test_result

@behave.then("for every call to next_state the remaining time of the minimal job is subtracted correctly")
def then_remaining_time(context):
    assert context.cluster.test_result

@behave.then("for every call to next_state the jobs with 0 remaining time are converted to EmptyJob instances")
def then_empty_jobs(context):
    assert context.cluster.test_result

@behave.then("for every call to next_state the first job for each execution unit has the largest number of binded cores")
def then_jobs_binded_cores(context):
    assert context.cluster.test_result
