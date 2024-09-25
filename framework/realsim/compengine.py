from realsim.cluster.abstract import AbstractCluster
from realsim.database import Database
from realsim.scheduler.scheduler import Scheduler


class ComputeEngineEventTarget:
    CLUSTER = 0
    DATABASE = 1
    SCHEDULER = 2

class ComputeEngineEvent:
    LOAD_IN_WAITING_QUEUE = "Load jobs in the pre-load state in the waiting queue of a cluster"


class ComputeEngine:

    def __init__(self, 
                 cluster: AbstractCluster,
                 database: Database,
                 scheduler: Scheduler):

        self.__targets = {
            ComputeEngineEventTarget.CLUSTER: cluster,
            ComputeEngineEventTarget.DATABASE: database,
            ComputeEngineEventTarget.SCHEDULER: scheduler
        }

    def handle_signal(self, signal, target):
        pass

    def sim_step(self):
        self.handle_signal(ComputeEngineEvent.LOAD_IN_WAITING_QUEUE, ComputeEngineEventTarget.CLUSTER)
