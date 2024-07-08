"""
The database will be responsible of storing a pre-loaded queue of jobs at the
initialisation of a simulation. It will also store important information about
jobs.

v1.0 : For starters, the information about the heatmap of the jobs in the
pre-loaded queue will be stored.

v1.1 (!next!) : store info about allocation of jobs (co-scheduling) so that the
execution list will be a list of floating number and not a list of lists. Also
the jobs will be addressed by their ids.
"""

import os
import sys
from typing import Optional, Protocol

# Set the root directory of the api library
sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../../"
)))

from api.loader import Load

# Set the root directory of the realsim library
sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../"
)))

# Import library components
from realsim.jobs.jobs import Job
from realsim.jobs.utils import deepcopy_list


# Define the inference engine
class InferenceEngine(Protocol):
    def predict(self, X):
        pass


class Database:

    def __init__(self, jobs_set: list[Job], engine: Optional[InferenceEngine] = None):
        self.preloaded_queue: list[Job] = deepcopy_list(jobs_set)
        self.engine: Optional[InferenceEngine] = engine
        self.heatmap: dict[str,dict[str,float]] = dict()

    def pop(self, queue: list[Job]) -> Job:
        job: Job = queue[0]
        queue.remove(job)
        return job

    def init_heatmap(self):

        # Initialize the heatmap
        for job in self.preloaded_queue:
            self.heatmap[job.job_name] = {}

        # Get a copy of the preloaded queue
        preloaded_queue = deepcopy_list(self.preloaded_queue)

        while preloaded_queue != []:

            job: Job = self.pop(preloaded_queue)

            load: Optional[Load] = job.load

            if load is None:
                raise RuntimeError("A job with an empty load was found inside the waiting queue at the startup stage")

            for co_job in preloaded_queue:

                co_load: Optional[Load] = co_job.load

                if co_load is None:
                    raise RuntimeError("A job with an empty load was found inside the waiting queue at the startup stage")

                if self.engine is not None:
                    # If an inference engine is provided then predict the
                    # speedup for both load and co-load when co-scheduled

                    # Get speedup for load when co-scheduled with co-load
                    self.heatmap[load.load_name].update({
                            co_load.load_name: self.engine.predict(
                                load.get_tag(), co_load.get_tag()
                            )
                    })

                    # Get speedup for co-load when co-scheduled with load
                    self.heatmap[co_load.load_name].update({
                            load.load_name: self.engine.predict(
                                co_load.get_tag(), load.get_tag()
                            )
                    })

                else:
                    # If we do not have an inference engine, then use the stored
                    # knowledge inside each load to get their speedups
                    # and if we do not have knowledge of their co-execution then
                    # submit a None value inside the heatmap

                    # Get speedup for load when co-scheduled with co-load
                    self.heatmap[load.load_name].update({

                            co_load.load_name:

                            load.get_med_speedup(co_load.load_name) 
                            if co_load.load_name in load.coscheduled_timelogs
                            else None

                    })

                    # Get speedup for co-load when co-scheduled with load
                    self.heatmap[co_load.load_name].update({

                            load.load_name:

                            co_load.get_med_speedup(load.load_name) 
                            if load.load_name in co_load.coscheduled_timelogs
                            else None

                    })

    def setup(self):
        self.init_heatmap()
