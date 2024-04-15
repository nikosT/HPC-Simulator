from typing import Optional
from .load import Load
from glob import glob
import os
import re
import math
from concurrent.futures import ProcessPoolExecutor
import pymongo
from pymongo.server_api import ServerApi
import pickle
from functools import reduce


class LoadManager:
    """
    Each load manager manages loads for a specific suite
    that ran on a specific machine
    """

    def __init__(self, machine, suite=None, rootdir = None):
        """Initialize a LoadManager instance

        ⟡ machine ⟡ the name of the machine on which the benchmarks were executed.
        The name can be the overall name of the HPC cluster or the name of the cluster
        and the name of the partition which was used. For example, 
        machine = "machineA.particionC"

        ⟡ ppn ⟡ the number of processors per node. This is a necessary value to 
        calculate the nodes that were necessary for each experiment to run

        ⟡ suite ⟡ the suite of benchmarks that we experimented on
        """
        self.rootdir: Optional[str] = rootdir

        if self.rootdir is None:
            self.rootdir = self.find_rootdir()

        self.machine: str = machine
        self.suite: Optional[str] = suite
        self.loads: dict[str, Load] = dict()

    def __call__(self, load: str) -> Load:
        # Else, return the specific instance of load
        return self.loads[load]

    def __iter__(self):
        return self.loads.items().__iter__()

    def __contains__(self, load: str) -> bool:
        return load in self.loads

    def __repr__(self) -> str:
        return "\033[1mLoads currently being managed\033[0m\n" + str(list(self.loads.keys()))

    def __str__(self) -> str:
        return "\033[1mLoads currently being managed\033[0m\n" + str(list(self.loads.keys()))


    def __getitem__(self, keys) -> 'LoadManager':
        """Get a load manager that manages a subset of loads; the subset is keys
        """

        ret_lm = self.deepcopy()

        # Remove all the loads that are not in keys
        for load in self.loads:
            if load not in keys:
                ret_lm.loads.pop(load)

        # Also do the same for coloads
        for ret_load in ret_lm.loads:
            coloads_removing = list()
            for ret_coload in ret_lm(ret_load).coloads:
                if ret_coload not in keys:
                    coloads_removing.append(ret_coload)
            for ret_coload in coloads_removing:
                ret_lm(ret_load).coloads.pop(ret_coload)

        return ret_lm

    def __add__(self, lm) -> 'LoadManager':

        ret_lm = LoadManager(self.machine, self.suite)
        # ret_lm.suite += "/" + lm.suite

        # Return an empty load manager
        if self.machine != lm.machine:
            return ret_lm

        # 1. Create a deep copy of the current load manager
        # by copying all the Loads
        for load in self.loads:
            ret_lm.loads.update({load: self.loads[load].deepcopy()})

        # 2. Add new Loads to our copied LoadManager instance if there aren't
        # or update our loads' coloads
        for load, _ in lm:
            if load not in ret_lm.loads:
                # If it doesn't exist then add new load
                ret_lm.loads.update({load: lm(load).deepcopy()})
            else:
                # If it does update the coloads of our ret_lm
                for coload in lm(load).coloads:
                    if coload not in ret_lm(load).coloads:
                        ret_lm(load).coloads[coload] = lm(load).coloads[coload].copy()

        return ret_lm

    def __iadd__(self, lm) -> 'LoadManager':
        return self.__add__(lm)

    def deepcopy(self) -> 'LoadManager':
        """Return a deepcopy of the load manager
        """
        ret_lm = LoadManager(machine=self.machine, suite=self.suite)
        for load in self.loads:
            ret_lm.loads[load] = self.loads[load].deepcopy()
        return ret_lm

    def find_rootdir(self) -> str:
        return glob(
                os.path.abspath(
                    os.path.join(os.path.dirname(__file__), '..', '..')
                ), recursive=True)[0]

    @staticmethod
    def to_seconds(runtime) -> float:
        sec = 0
        timestamps = runtime.split(':')[::-1]
        for i, t in enumerate(timestamps):
            sec += float(t) * (60 ** i)
        return sec

    @staticmethod
    def init_compact(cmp_dir_bundle) -> tuple[str, str, list[float]]:
        """Gather all the necessary data from the compact experiments,
        create each load and initialize their execution time bundles

        ⟡ runs_dir ⟡ the directory to which the output logs of the
        experiments are saved
        """

        # Deconstruct input
        suite, cmp_dir = cmp_dir_bundle

        # Get the name of a load from the directory's name
        load = os.path.basename(cmp_dir).replace("_cmp", "")
        # Create the corresponding Load
        # self.loads[load] = Load(load)

        # Check if the log file for the specific
        # load exists
        try:
            files = os.listdir(cmp_dir)
            file = list(filter(lambda f: "_cmp" in f, files))[0]
        except IndexError:
            # If not print that nothing was found
            # and continue to the next directory
            print(f"No log file found inside {cmp_dir}")
            return load, suite, []

        # Open the log file of the compact
        # experiment on the load
        fd = open(cmp_dir + "/" + file, "r")

        time_logs = list()
        for line in fd.readlines():
            if "Time in seconds" in line:
                time_logs.append(float(line.split()[-1]))
            if "Overall Time:" in line:
                time_logs.append(LoadManager.to_seconds(line.split()[-1]))

        fd.close()

        return load, suite, time_logs

    @staticmethod
    def init_coschedule(cos_dir) -> list[tuple[str, str, list[float]]]:
        """Get the execution times of a load and coload in a coscheduled
        experiment. The function is static because it is called in parallel
        and also because logically it can be called beside's a LoadManager's
        instantiation.

        ⟡ cos_dir_bundle ⟡ provide the suite and the directory of the
        coscheduled experiment; based on the suite we can define different
        tactics to extract the run times of the load and coload; based
        on the directory we know were to find the logs of the experiment

        ⟡⟡ returnVal ⟡⟡ A list with the following scheme:
        [
            [loadA, loadB, loadA_runtimes_besides_loadB],
            [loadB, loadA, loadB_runtimes_besides_loadA],
        ]

        A dictionary was avoided because of how many coscheduled 
        experiments have the same benchmarks as load and coload.
        The definition of keys would overlap with each other.
        """

        # A list will be returned
        # A dictionary was avoided because of how many
        # coscheduled experiments have the same benchmarks
        # as load and coload
        out = list()

        # Discern the individual names of the loads
        loads = re.split(r'(.+\d+)_', os.path.basename(cos_dir))
        loads = list(filter(None, loads))
        first_load, second_load = loads

        first_name, first_num_of_processes = list(filter(None, re.split(r'(.+)\.(\d+)', first_load)))
        first_files = [cos_dir + '/' + file
                       for file in os.listdir(cos_dir)
                       if re.match('^' + first_name, file)]

        second_name, second_num_of_processes = list(filter(None, re.split(r'(.+)\.(\d+)', second_load)))
        second_files = [cos_dir + '/' + file
                        for file in os.listdir(cos_dir)
                        if re.match('^' + second_name, file)]

        # If the loads are the same then exclude
        # the same logs
        if first_load == second_load:
            first_files = [first_files[0]]
            second_files = [second_files[1]]
        else:
            # If the first and second load are the same but different
            # with respect to their number of processes requested
            # then for the first load allow logs of the same number of processes
            # and the same thing for the second load, accountably
            if first_name == second_name:
                files_to_remove = set()
                for file in first_files:
                    fd = open(file)
                    for line in fd.readlines():
                        if 'Total number of processes' in line or 'Total processes' in line:
                            # If the number of processes is not the same
                            # as the one stated inside the file then this
                            # log is not a log of the first load
                            if line.split()[-1] != first_num_of_processes:
                                fd.close()
                                files_to_remove.add(file)
                                continue

                for file in files_to_remove:
                    first_files.remove(file)

                # The files needed to be removed from
                # the first load are the necessary logs
                # of the second load
                second_files = list(files_to_remove)

        # f_l_times, f_load_times, f_load_cos_times
        # first_time_logs = []
        f_load_cos_times = list()
        for file in first_files:
            logfile_times = list()
            with open(file) as fd:
                for line in fd.readlines():
                    if "Time in seconds" in line:
                        logfile_times.append(float(line.split()[-1]))
                    if "Overall Time:" in line:
                        logfile_times.append(LoadManager.to_seconds(line.split()[-1]))
            f_load_cos_times.append(logfile_times)

        # second_time_logs = []
        s_load_cos_times = list()
        for file in second_files:
            logfile_times = list()
            with open(file) as fd:
                for line in fd.readlines():
                    if "Time in seconds" in line:
                        logfile_times.append(float(line.split()[-1]))
                    if "Overall Time:" in line:
                        logfile_times.append(LoadManager.to_seconds(line.split()[-1]))
            s_load_cos_times.append(logfile_times)

        # If the same workloads then get the same lists
        # of coscheduled times
        if first_load == second_load:
            s_load_cos_times += f_load_cos_times
            f_load_cos_times = s_load_cos_times

        out.append((first_load, second_load, f_load_cos_times))
        out.append((second_load, first_load, s_load_cos_times))

        return out

    def init_loads(self, runs_dir=None) -> None:
        """Create and initialize the time bundles of loads of a specified
        benchmark suite on a specified machine. Firstly, it creates the
        loads. Secondly, it populates their compact run time bundles.
        Lastly, it bonds together different loads based on the coscheduled
        experiments that were ran and saves their run time bundles for
        each pair.

        ⟡ runs_dir ⟡ if a user needs to point manually where the loads are
        saved; if not then the process of finding them becomes automatic
        and is based on the directory tree structure of the project
        """
        if runs_dir is None:
            runs_dir = f"{self.rootdir}/Co-Scheduling/logs"

        if self.suite is None:
            raise RuntimeError("A suite name was not given")

        # If suites were mixed on the experiments then 
        # get their compact counterparts from their
        # respective direcories
        if "_" in self.suite:
            compact_dirs_bundle = list()
            masks = os.listdir(f"{runs_dir}/{self.machine}/{self.suite}")
            for suite in self.suite.split("_"):
                compact_dirs_bundle.extend([
                    (suite, f"{runs_dir}/{self.machine}/{suite}/{dire}")
                    for dire in os.listdir(f"{runs_dir}/{self.machine}/{suite}")
                    if '_cmp' in dire and
                    reduce(lambda a, b: a or b, map(lambda d: dire.replace("_cmp", "") in d, masks))
                ])
        else:
            # Get the compact experiments' directories
            compact_dirs_bundle = [
                (self.suite, f"{runs_dir}/{self.machine}/{self.suite}/{dire}")
                for dire in os.listdir(f"{runs_dir}/{self.machine}/{self.suite}")
                if '_cmp' in dire
            ]

        # Gather all the data from the compact runs of each load
        with ProcessPoolExecutor() as pool:
            res = pool.map(LoadManager.init_compact, compact_dirs_bundle)
            for name, suite, time_logs in res:
                if time_logs != []:
                    self.loads[name] = Load(name, suite)
                    self.loads[name].compact_time_bundle = time_logs

        # Get the coschedule experiments' directories
        coschedule_dirs = [
            f"{runs_dir}/{self.machine}/{self.suite}/{dire}"
            for dire in os.listdir(f"{runs_dir}/{self.machine}/{self.suite}")
            if '_cmp' not in dire and 'spare' not in dire
        ]

        # Gather all the data from the coscheduled runs of each load
        with ProcessPoolExecutor() as pool:
            res = pool.map(LoadManager.init_coschedule, coschedule_dirs)
            for elem in res:
                first_load_list, second_load_list = elem
                first_load, first_coload, first_time_logs = first_load_list
                second_load, second_coload, second_time_logs = second_load_list

                try:
                    self.loads[first_load].set_coload(first_coload, first_time_logs)
                except Exception:
                    print(f"\033[31m{self.machine} : {self.suite} -> {first_load}: Couldn't build load\033[0m")
                    pass

                try:
                    self.loads[second_load].set_coload(second_coload, second_time_logs)
                except Exception:
                    print(f"\033[31m{self.machine} : {self.suite} -> {second_load}: Couldn't build load\033[0m")
                    pass

    def profiling_data(self, ppn, profiling_dir=None) -> None:

        """Gather all the perf and mpiP data and save them to their
        respective loads

        ⟡ ppn ⟡ processors per node; used to calculate the nodes binded
        by a load

        ⟡ profiling_dir ⟡ if someone wants to set manually where the
        perf and mpiP logs are kept for each load
        """

        # If the directory where all the perf and mpiP are kept is not
        # manually provided by the user then search for the project's
        # root directory. If rootdir already has a value then proceed
        # with the definition of profiling_dir
        if profiling_dir is None:
            if self.rootdir is None:
                self.find_rootdir()
            profiling_dir = f"{self.rootdir}/Performance_Counters/logs"

        # Setup the logs directory for the specific machine
        # and benchmark suite
        logs_dir = f"{profiling_dir}/{self.machine}/{self.suite}"

        # Check if the directory exists
        if not os.path.exists(logs_dir):
            return
        
        for load_dir in os.listdir(logs_dir):

            # Every directory's name is a load
            load = load_dir
            # We need to know how many nodes where binded for the experiment
            nodes_binded = math.ceil(self.loads[load].num_of_processes / ppn)

            # Open and get the perf logs for the specific load
            # The perf logs are located inside a file called PERF_COUNTERS
            try:
                fd = open(f"{logs_dir}/{load_dir}/EXTRACTED/PERF_COUNTERS", "r")
                # cycles: how many CPU cycles were consumed
                # to execute the load
                cycles = int(fd.readline().split(':')[1])
                # instructions: how many CPU specific instructions 
                # were executed for the load
                instructions = int(fd.readline().split(':')[1])
                # dpops: how many double precision floating point
                # operations were executed when we ran the load
                dpops = int(fd.readline().split(':')[1])
                # bytes_transferred: how many bytes were transferred when
                # we executed the load
                bytes_transferred = 64 * int(fd.readline().split(':')[1])
                fd.close()

                # From the previous values we get ipc, dpops per node
                # and bytes_transferred per node for each load
                # The last two are divided by the number of nodes
                # to include impartial architectural characteristics
                # to the experiments
                self.loads[load].ipc = instructions / cycles
                self.loads[load].dpops = dpops / nodes_binded
                self.loads[load].bytes_transferred = bytes_transferred / nodes_binded

            except Exception:
                print(f"\033[33m{load} -> EXTRACTED/PERF_COUNTERS: File doesn't exist\033[0m")

            # Open and get information about the compute and MPI times
            # spent on each load
            # The values are saved on a file called LOAD_AGGR_TIME
            try:
                fd = open(f"{logs_dir}/{load_dir}/EXTRACTED/LOAD_AGGR_TIME", "r")
                app_time = float(fd.readline().split(':')[1])
                mpi_time = float(fd.readline().split(':')[1])
                fd.close()

                # Compute time = app_time - mpi_time
                self.loads[load].compute_time = app_time - mpi_time
                self.loads[load].mpi_time = mpi_time

                # Also add the percentage
                self.loads[load].compute_perc = (app_time - mpi_time) / app_time
                self.loads[load].mpi_perc = mpi_time / app_time

            except Exception:
                print(f"\033[33m{load} -> EXTRACTED/LOAD_AGGR_TIME : File doesn't exist\033[0m")

            # Open and get which and how many times specific MPI
            # functions were called
            # The values can be found on a file called MPI_CMDS_CALLS
            try:
                fd = open(f"{logs_dir}/{load_dir}/EXTRACTED/MPI_CMDS_CALLS")
                for line in fd.readlines():
                    # The line is formatted as MPI cmd:value
                    mpi_cmd, val = line.split(':')
                    # Process the fields
                    mpi_cmd = f"mpi_{mpi_cmd.lower()}"
                    val = int(float(val))
                    # Save the values
                    self.loads[load].noc[mpi_cmd] = val
                fd.close()

            except Exception:
                print(f"\033[33m{load} -> EXTRACTED/MPI_CMDS_CALLS : File doesn't exist\033[0m")

            try:
                fd = open(f"{logs_dir}/{load_dir}/EXTRACTED/MPI_CMDS_TIME")
                for line in fd.readlines():
                    # The line is formatted as MPI cmd:value
                    mpi_cmd, val = line.split(':')
                    # Process the fields
                    mpi_cmd = f"mpi_{mpi_cmd.lower()}"
                    val = float(val)
                    # Save the values
                    self.loads[load].atime[mpi_cmd] = val
                fd.close()

            except Exception:
                print(f"\033[33m{load} -> EXTRACTED/MPI_CMDS_TIME : File doesn't exist\033[0m")

            try:
                fd = open(f"{logs_dir}/{load_dir}/EXTRACTED/MPI_CMDS_BYTES")
                for line in fd.readlines():
                    # The line is formatted as MPI cmd:value
                    mpi_cmd, val = line.split(':')
                    # Process the fields
                    mpi_cmd = f"mpi_{mpi_cmd.lower()}"
                    val = int(float(val))
                    # Save the values
                    self.loads[load].abytes[mpi_cmd] = val
                fd.close()

            except Exception:
                print(f"\033[33m{load} -> EXTRACTED/MPI_CMDS_BYTES : File doesn't exist\033[0m")

    def export_to_db(self, 
                     host="localhost", 
                     port=8080, 
                     username=None, 
                     password=None, 
                     dbname=None, 
                     collection="loads") -> None:

        # Get the credentials from the user in order to hide
        # them from the source code
        try:
            # Create a Mongo client to communicate with the database
            if "mongodb+srv://" in host or "mongodb://" in host:
                client = pymongo.MongoClient(host, server_api=ServerApi("1"))
            else:

                if username is None:
                    raise RuntimeError("Didn't provide a username")
                if password is None:
                    raise RuntimeError("Didn't provide a password")

                client = pymongo.MongoClient(host, port, username=username, password=password)
        except Exception:
            print("Couldn't connect to MongoServer. Is the server up?")
            return

        # Connect to or create the database
        if dbname is None:
            raise Exception("Please provide a database name")

        db = client[dbname]

        # Get reference or create the 'loads' collection
        coll = db[collection]

        # Add or update a load
        for load in self.loads:
            # Create the id of the load
            _id = {
                "machine": self.machine,
                "suite": self.loads[load].suite,
                "load": load
            }

            # First check if the load already exists in
            # the collection
            query = {"_id": _id}

            # Get a list of loads with the same id
            findings = list(coll.find(query))

            if findings != []:
                # If the load exists then update all occurencies
                coll.update_many(query, {"$set": {"bin": pickle.dumps(self.loads[load])}})
            else:
                coll.insert_one({"_id": _id,  "bin": pickle.dumps(self.loads[load])})

    def import_from_db(self, 
                       host="localhost", 
                       port=8080, 
                       username=None, 
                       password=None, 
                       dbname=None, 
                       collection="loads") -> None:

        try:
            # Create a Mongo client to communicate with the database
            if "mongodb+srv://" in host or "mongodb://" in host:
                client = pymongo.MongoClient(host, server_api=ServerApi("1"))
            else:

                if username is None:
                    raise RuntimeError("Didn't provide a username")
                if password is None:
                    raise RuntimeError("Didn't provide a password")

                client = pymongo.MongoClient(host, port, username=username, password=password)

        except Exception:
            print("Couldn't connect to MongoServer. Is the server up?")
            return

        # Connect to or create the database
        if dbname is None:
            raise Exception("Please provide a database name")
        db = client[dbname]

        # Get reference or create the 'loads' collection
        coll = db[collection]

        # Query based on machine and/or suite
        if self.suite is not None:
            query = { "_id.machine": self.machine, "_id.suite": self.suite }
        else:
            query = { "_id.machine": self.machine }

        for doc in coll.find(query):
            load = pickle.loads(doc["bin"])
            load.coloads_median_speedup = dict()
            for coload_name in load.coloads:
                load.set_median_speedup(coload_name)
            self.loads[doc["_id"]["load"]] = load

        # Filter out coloads
        for _, load in self.loads.items():

            correct_coloads = dict()

            for coload_name in load.coloads:
                if coload_name in self.loads:
                    correct_coloads[coload_name] = load.coloads[coload_name]

            load.coloads = correct_coloads
