from typing import Optional
from numpy import average as avg
from numpy import median


class Load:

    def __init__(self, full_load_name: str, suite: Optional[str] = None):

        # Load's full name and products
        self.full_load_name = full_load_name
        try:
            self.benchmark = full_load_name.split('.')[0]
            self.cclass = full_load_name.split('.')[1]
            self.num_of_processes = int(full_load_name.split('.')[2])
        except Exception:
            pass

        # Load's suite
        self.suite = suite

        # Load's compact attributes
        self.compact_time_bundle = []

        # Perf events
        self.dpops = 0
        self.bytes_transferred = 0
        self.ipc = 0

        # MPI features
        # MPI overall time
        self.compute_time = 0
        self.mpi_time = 0
        self.compute_perc = 0
        self.mpi_perc = 0

        # MPI events : number of calls
        self.noc = dict()
        self.noc["mpi_allgather"] = 0
        self.noc["mpi_allreduce"] = 0
        self.noc["mpi_alltoall"] = 0
        self.noc["mpi_barrier"] = 0
        self.noc["mpi_bcast"] = 0
        self.noc["mpi_comm_dup"] = 0
        self.noc["mpi_comm_free"] = 0
        self.noc["mpi_comm_split"] = 0
        self.noc["mpi_dims_create"] = 0
        self.noc["mpi_irecv"] = 0
        self.noc["mpi_isend"] = 0
        self.noc["mpi_recv"] = 0
        self.noc["mpi_reduce"] = 0
        self.noc["mpi_scan"] = 0
        self.noc["mpi_send"] = 0
        self.noc["mpi_wait"] = 0
        self.noc["mpi_waitall"] = 0

        # MPI events : aggregated time (milliseconds)
        self.atime = dict()
        self.atime["mpi_allgather"] = 0
        self.atime["mpi_allreduce"] = 0
        self.atime["mpi_alltoall"] = 0
        self.atime["mpi_barrier"] = 0
        self.atime["mpi_bcast"] = 0
        self.atime["mpi_comm_dup"] = 0
        self.atime["mpi_comm_free"] = 0
        self.atime["mpi_comm_split"] = 0
        self.atime["mpi_dims_create"] = 0
        self.atime["mpi_irecv"] = 0
        self.atime["mpi_isend"] = 0
        self.atime["mpi_recv"] = 0
        self.atime["mpi_reduce"] = 0
        self.atime["mpi_scan"] = 0
        self.atime["mpi_send"] = 0
        self.atime["mpi_wait"] = 0
        self.atime["mpi_waitall"] = 0

        # MPI events : aggregated bytes
        self.abytes = dict()
        self.abytes["mpi_allgather"] = 0
        self.abytes["mpi_allreduce"] = 0
        self.abytes["mpi_alltoall"] = 0
        self.abytes["mpi_barrier"] = 0
        self.abytes["mpi_bcast"] = 0
        self.abytes["mpi_comm_dup"] = 0
        self.abytes["mpi_comm_free"] = 0
        self.abytes["mpi_comm_split"] = 0
        self.abytes["mpi_dims_create"] = 0
        self.abytes["mpi_irecv"] = 0
        self.abytes["mpi_isend"] = 0
        self.abytes["mpi_recv"] = 0
        self.abytes["mpi_reduce"] = 0
        self.abytes["mpi_scan"] = 0
        self.abytes["mpi_send"] = 0
        self.abytes["mpi_wait"] = 0
        self.abytes["mpi_waitall"] = 0

        # Load's coschedule attributes
        # coloads = dict(coload_full_name: load_coscheduled_time_bundle)
        self.coloads: dict[str, list] = dict()

        self.coloads_median_speedup: dict[str, float] = dict()

    def __repr__(self) -> str:
        return self.__str__()

    def __str__(self) -> str:
        return f"""\033[1mLoad:\033[0m
⊙ Suite: {self.suite}
⊙ Benchmark: {self.benchmark}
⊙ Class: {self.cclass}
⊙ Number of processes: {self.num_of_processes}
⊙ Avg DP FLOPs/s: {(self.get_avg_dp_FLOPS() / 10 ** 9):.4f} GFLOPS
⊙ Avg Bytes/s: {(self.get_avg_dram_bandwidth() / 2 ** 30):.4f} GB/s
⊙ Avg IPC: {self.ipc / self.get_avg()}
⊙ Coloads: {list(self.coloads.keys())}"""

    def __call__(self, coload=None) -> list[float]:
        """Return execution time bundle of load

        ▛ coload ▟ if coload is given then return the execution time bundle of
        load when it is coscheduled with coload. If no coload is given return
        the time bundle of load when it is executed with compact policy
        """
        if coload is None:
            # Return the time bundle
            # of compact execution
            return self.compact_time_bundle
        else:
            # Return the time bundle when
            # load is coscheduled with coload
            return self.coloads[coload]

    def __eq__(self, load) -> bool:
        if not isinstance(load, Load):
            return False
        return self.full_load_name == load.full_load_name\
                and self.suite == load.suite\
                and self.bytes_transferred == load.bytes_transferred\
                and self.ipc == load.ipc\
                and self.compact_time_bundle == load.compact_time_bundle\
                and self.dpops == load.dpops\
                and self.compute_time == load.compute_time and self.compute_perc == load.compute_perc\
                and self.mpi_time == load.mpi_time and self.mpi_perc == load.mpi_perc\
                and self.noc == load.noc\
                and self.atime == load.atime\
                and self.abytes == load.abytes\
                and self.coloads == load.coloads

    def deepcopy(self) -> 'Load':

        # Create a Load instance
        ret_load = Load(self.full_load_name, self.suite)

        # Copy load's attributes to ret_load
        ret_load.compact_time_bundle = self.compact_time_bundle.copy()
        ret_load.dpops = self.dpops
        ret_load.bytes_transferred = self.bytes_transferred
        ret_load.ipc = self.ipc
        ret_load.compute_time = self.compute_time
        ret_load.mpi_time = self.mpi_time
        ret_load.compute_perc = self.compute_perc
        ret_load.mpi_perc = self.mpi_perc

        # Deep copy of MPI attributes' dicts
        ret_load.noc = dict()
        ret_load.noc.update(self.noc)
        ret_load.atime = dict()
        ret_load.atime.update(self.atime)
        ret_load.abytes = dict()
        ret_load.abytes.update(self.abytes)

        # Deep copy of coloads
        ret_load.coloads = dict()
        for coload in self.coloads:
            key = coload
            value = self.coloads[key].copy()
            ret_load.coloads[key] = value

        return ret_load

    def get_avg(self, coload=None) -> float:
        """Return the average execution time of a load

        ▛ coload ▟ if coload is given then return the average execution time
        of the load when it is coscheduled with coload. If no coload is given
        return the average time of the load when it is executed with
        compact policy
        """
        if coload is None:
            # Get compact average
            return float(
                    avg(self.compact_time_bundle)
            )
        else:
            # Get average time when load is
            # coscheduled with coload
            return float(
                    avg(list(map(lambda li: avg(li), self.coloads[coload])))
            )

    def get_median(self, coload=None) -> float:
        """Return the median execution time of a load

        ▛ coload ▟ if coload is given then return the median execution time
        of the load when it is coscheduled with coload. If no coload is given
        return the median time of the load when it is executed with
        compact policy
        """
        if coload is None:
            # Get compact median time
            return float(
                    median(self.compact_time_bundle)
            )
        else:
            # Get median time of load
            # when it's coscheduled with coload
            return float(
                    avg(list(map(lambda li: median(li), self.coloads[coload])))
            )

    def get_avg_speedup(self, coload) -> float:
        """Return the average speedup of a load when coscheduled with coload

        ▛ coload ▟ the load colocated to the same nodes with load

        ▛ ReturnVal ▟ returns the average speedup of load when coscheduled 
        with coload
        """
        return (self.get_avg() / self.get_avg(coload))

    def get_median_speedup(self, coload) -> float:
        """Return the average speedup of a load when coscheduled with coload

        ▛ coload ▟ the load colocated to the same nodes with load

        ▛ ReturnVal ▟ returns the average speedup of load when coscheduled 
        with coload
        """
        return self.coloads_median_speedup[coload]

    def set_median_speedup(self, coload):
        """Return the average speedup of a load when coscheduled with coload

        ▛ coload ▟ the load colocated to the same nodes with load

        ▛ ReturnVal ▟ returns the average speedup of load when coscheduled 
        with coload
        """
        self.coloads_median_speedup[coload] = (self.get_median() /self.get_median(coload))

    def get_dram_bandwidth_list(self) -> list[float]:
        """Get the DRAM bandwidth of load for each run

        ▛ ReturnVal ▟ returns a list with the DRAM bandwidth for a load
        """
        return list(
            map(lambda x: self.bytes_transferred / x, self.compact_time_bundle)
        )

    def get_avg_dram_bandwidth(self) -> float:
        """Get the average DRAM bandwidth of a load

        ▛ ReturnVal ▟ returns the average DRAM bandwidth of a load
        """
        return float( avg(self.get_dram_bandwidth_list()) )

    def get_dp_FLOPS_list(self) -> list[float]:
        """Get the double precision FLOPS of load for each run

        ▛ ReturnVal ▟ returns a list with double precision FLOPS for a load
        """
        return list(
            map(lambda x: self.dpops / x, self.compact_time_bundle)
        )

    def get_avg_dp_FLOPS(self) -> float:
        """Get the average double precision FLOPS of a load

        ▛ ReturnVal ▟ returns the average double precision FLOPS of a load
        """
        return float( avg(self.get_dp_FLOPS_list()) )

    def get_tag(self) -> list:
        return [self.get_median(), 
                self.compute_perc, 
                self.mpi_perc, 
                self.ipc, 
                self.get_avg_dp_FLOPS(), 
                self.get_avg_dram_bandwidth()]

    def set_coload(self, coload, time_bundle=[]) -> None:
        """Save the name of a coscheduled load and the execution time for each
        run of the load

        ▛ coload ▟ the load colocated to the same nodes with load

        ▛ time_bundle ▟ the execution times for each run of the load

        ▛ ReturnVal ▟ noreturn
        """
        self.coloads[coload] = time_bundle
