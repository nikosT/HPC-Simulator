from json import loads
from typing import Protocol


class ConfigScript:

    def __init__(self, path_to_script: str):
        
        # Load the configuration file
        with open(path_to_script, "r") as fd:
            data = loads(fd.read())
            
            if "name" not in data or "workloads" not in data or "schedulers" not in data:
                raise RuntimeError("The configuration file is not properly designed")

            self.project_name = data["name"]
            self.workloads = data["workloads"]
            self.schedulers = data["schedulers"]
            self.actions = data["actions"] if "actions" in data else list()

            print("The total number of MPI ranks is ", len(self.workloads) * (1 + len(self.schedulers["others"])))

    def process_workloads(self):
        pass

    def create_ranks(self):
        pass



ConfigScript("./project.json")
