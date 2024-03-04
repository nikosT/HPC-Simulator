import math
from realsim.cluster.shallow import ClusterShallow
from .scheduler import Scheduler
from realsim.jobs import Job, EmptyJob
from realsim.jobs.utils import deepcopy_list
from typing import List, Dict, Tuple
from numpy import average as avg


class Balancer(Scheduler):
    """We try to provide at every checkpoint an execution list whose average
    speedup is higher than 1. We try to distribute the higher speedup candidates
    among the checkpoints.
    """

    name = "Balancer"
    description = "Balancing out the speedups between checkpoints"

    def __init__(self):
        # Square matrix with speedups for each pairing
        self.pairs_matrix: Dict[int, Dict] = dict()
        Scheduler.__init__(self)

        # Declare the rank system
        self.ranks: dict[str, int]

    def assign_cluster(self, cluster: ClusterShallow):
        """This method is called from a cluster instance
        when it is created. It can also be used to reassign
        the scheduler to other clusters. It is essential for
        rapid experimenting.
        """
        self.cluster = cluster
        self.ranks = dict()
        self.update_ranks()

    def pmatrix_init(self) -> None:
        """Create a triagonal matrix that stores the all the 
        possible pairs of the jobs in waiting queue and the 
        executing jobs in half pairs
        """

        # Start from scratch
        self.pairs_matrix = dict()

        # Create a copy of the cluster's waiting queue
        jobs_list = deepcopy_list(self.cluster.waiting_queue)
        
        # Get the jobs from execution list half pairs
        half_pairs_jobs = deepcopy_list([
            job for job, _ in self.exec_half_pairs()
        ])

        # Extend jobs_list
        jobs_list.extend(half_pairs_jobs)

        for i, job in enumerate(jobs_list):
            job_id = job.job_id
            job_row = dict()
            for cojob in jobs_list[i+1:]:

                if cojob.load.full_load_name not in job.load.coloads:
                    continue

                cojob_id = cojob.job_id

                job_speedup = job.get_speedup(cojob)
                cojob_speedup = job.get_speedup(job)
                pair_speedup = avg([job_speedup, cojob_speedup])

                job_half_needed_cores = self.cluster.half_node_cores(job)
                cojob_half_needed_cores = self.cluster.half_node_cores(cojob)
                pair_half_needed_cores = job_half_needed_cores
                if cojob_half_needed_cores > job_half_needed_cores:
                    pair_half_needed_cores = cojob_half_needed_cores

                job_row[cojob_id] = {
                        'speedup': pair_speedup,
                        'needed cores': pair_half_needed_cores
                        }

            self.pairs_matrix[job_id] = job_row

    def pmatrix_get_speedup(self, job_id, cojob_id):
        if cojob_id in self.pairs_matrix[job_id]:
            return self.pairs_matrix[job_id][cojob_id]["speedup"]
        elif job_id in self.pairs_matrix[cojob_id]:
            return self.pairs_matrix[cojob_id][job_id]["speedup"]
        else:
            # For every pair that doesn't exists return -1
            return -1

    def exec_half_pairs(self) -> List[List[Job]]:
        """Return a list with pairs that their partner has finished 
        executing
        """
        half_pairs_list = list()
        for item in self.cluster.execution_list:
            if len(item) == 2 and type(item[0]) == Job and type(item[1]) == EmptyJob:
                half_pairs_list.append(item)

        return half_pairs_list

    def exec_full_pairs(self) -> List[List[Job]]:
        """Return a list with pairs that their partner has finished 
        executing
        """
        full_pairs_list = list()
        for item in self.cluster.execution_list:
            if len(item) == 2 and type(item[0]) == Job and type(item[1]) == Job:
                full_pairs_list.append(item)

        return full_pairs_list

    def exec_compact(self) -> List[List[Job]]:
        compact_list = list()
        for item in self.cluster.execution_list:
            if len(item) == 1 and type(item) != EmptyJob:
                compact_list.append(item)

        return compact_list

    def exec_avg_speedup(self) -> Tuple[int,float]:
        """Return the number and average speedup of jobs still
        executing in full pairs or compact
        """
        speedups = list()
        # Get all the compact jobs still executing
        for _ in self.exec_compact():
            speedups.append(1)

        # Get all full pairs still executing
        for item in self.exec_full_pairs():
            speedups.append(
                    self.pair_avg_speedup(item[0], item[1])
            )

        if speedups == []:
            return 0, 0.0
        else:
            return len(speedups), float(avg(speedups))

    def pair_avg_speedup(self, job: Job, cojob: Job):
        return avg([job.get_speedup(cojob), cojob.get_speedup(job)])

    def exec_ordering(self, job: Job, cojob: Job, ll_avg_speedup):

        # Get rank for job
        # The lowest the rank the more we have to get rid
        # of it fast
        rank = self.ranks[job.job_id]
        # If no pair is capable then put this possible pairing
        # to the lowest in list
        if rank == 0:
            return math.inf

        # Find out if they have the same cores
        # A possible pair should rise higher when they have the 
        # same needed cores
        same_cores = 1
        if self.cluster.half_node_cores(job) != cojob.binded_cores:
            same_cores = 0

        # Get max needed cores for colocation
        max_needed_cores = 2 * max(
                self.cluster.half_node_cores(job),
                cojob.binded_cores
        )

        # How many times can the pair fit in the cluster
        # The greater the number the less important because 
        # in execution co-location we want the bigger in cores needs
        # to rise higher in the list
        cores_ratio = self.cluster.total_cores / max_needed_cores

        # Get the average speedup of the pair
        # The higher the speedup the better
        avg_speedup = avg([
            job.get_speedup(cojob),
            cojob.get_speedup(job)
            ])

        if ll_avg_speedup > 0:
            speedup_rank = avg_speedup ** (2 / ll_avg_speedup)
        else:
            speedup_rank = avg_speedup

        #return ((4 ** same_cores) * speedup_rank / ((rank ** 2) * cores_ratio))
        return (4 ** same_cores) * speedup_rank * cores_ratio / (rank ** 2)

    def wait_ordering(self, job: Job, cojob: Job, ll_avg_speedup):

        # Get rank for job
        # The lowest the rank the more we have to get rid
        # of it fast
        rank = self.ranks[cojob.job_id]
        # If no pair is capable then put this possible pairing
        # to the lowest in list
        if rank == 0:
            return -1

        # Find out if they have the same cores
        # A possible pair should rise higher when they have the 
        # same needed cores
        same_cores = 1
        if self.cluster.half_node_cores(job) != cojob.binded_cores:
            same_cores = 0

        # Get max needed cores for colocation
        max_needed_cores = 2 * max(
                self.cluster.half_node_cores(job),
                cojob.binded_cores
        )

        # How many times can the pair fit in the cluster
        # The greater the number the less important because 
        # in execution co-location we want the bigger in cores needs
        # to rise higher in the list
        cores_ratio = self.cluster.total_cores / max_needed_cores

        # Get the average speedup of the pair
        # The higher the speedup the better
        avg_speedup = avg([
            job.get_speedup(cojob),
            cojob.get_speedup(job)
        ])

        if ll_avg_speedup > 0:
            speedup_rank = avg_speedup ** (2 / ll_avg_speedup)
        else:
            speedup_rank = avg_speedup

        # TODO
        # I have to bind the ll_avg_speedup with avg_speedup to push up
        # the higher speedups if a deploy list needs it
        # TODO

        #return (4 ** same_cores) * speedup_rank / ((rank ** 2) * cores_ratio)
        #return (4 ** same_cores) * speedup_rank * cores_ratio * rank
        return speedup_rank * cores_ratio * rank

    def update_ranks(self):
        """When deploying a list of jobs for execution then update
        the rankings
        """
        # Update the pairs matrix
        self.pmatrix_init()

        # New ranks
        self.ranks = dict()

        # Init rankings with zeroes
        for job_id in self.pairs_matrix:
            self.ranks[job_id] = 0

        # Count how many pairs
        for job_id in self.pairs_matrix.keys():
            for cojob_id in self.pairs_matrix[job_id].keys():
                if self.pmatrix_get_speedup(job_id, cojob_id) > 1:
                    self.ranks[job_id] += 1
                    self.ranks[cojob_id] += 1

    def deploying_exec_pairs(self, deploy_list, ll_num, ll_avg_speedup):

        ########################################
        # Colocation with executing half pairs #
        ########################################

        half_pairs = self.exec_half_pairs()

        # Sort them by rank
        half_pairs.sort(
                key=(lambda item: self.ranks[item[0].job_id]),
                reverse=True
        )

        for job, empty_job in half_pairs:

            # Every half pair will be degrading eventually to a pair
            # of lesser binded cores needs
            leq_binded_cores = list(
                    filter(
                        lambda cojob:
                        cojob.load.full_load_name in job.load.coloads and\
                        self.cluster.half_node_cores(cojob) <= job.binded_cores,
                        self.cluster.waiting_queue
                    )
            )

            # If no candidate co-job is found then recalculate pair as spread
            if leq_binded_cores == []:

                self.cluster.execution_list.remove([job, empty_job])

                # Check if the pair is already in a spread executing state
                if job.speedup != job.get_max_speedup():
                    job.remaining_time *= job.speedup / job.get_max_speedup()
                    job.speedup = job.get_max_speedup()

                # Calculate building list overall speedup
                ll_avg_speedup = (ll_avg_speedup * ll_num + job.speedup) / (ll_num + 1)
                ll_num += 1

                self.cluster.execution_list.append([job, empty_job])

                continue

            
            # Sort waiting jobs by an ordering function
            leq_binded_cores.sort(
                    key=(lambda cojob: self.wait_ordering(job, cojob, ll_avg_speedup)),
                    reverse=True
            )

            # Remove the former pair from the execution list
            self.cluster.execution_list.remove([job, empty_job])

            # Get first item === best match
            best_cojob = leq_binded_cores[0] 

            # Remove job from waiting queue
            self.cluster.waiting_queue.remove(best_cojob)

            # Recalculate ranks
            self.update_ranks()
            
            # Setup the job and cojob and push them to the deploying list
            best_cojob.ratioed_remaining_time(job)
            best_cojob.binded_cores = job.binded_cores

            job.ratioed_remaining_time(best_cojob)

            deploy_list.append([job, best_cojob])

            # Calculate ll_avg_speedup
            ll_avg_speedup = (ll_avg_speedup * ll_num +
                              self.pair_avg_speedup(job, best_cojob)) / (ll_num + 1)
            ll_num += 1

            # Write down event to logger
            self.logger.cluster_events["deploying:exec-colocation"] += 1

        return ll_num, ll_avg_speedup

    def deploying_wait_pairs(self, deploy_list, ll_num, ll_avg_speedup):
        """Coschedule jobs from the waiting queue

        + deploy_list: list of jobs to deploy to the execution list
        + ll_num: number of jobs/pairs in the execution list
        + ll_avg_speedup: the total average speedup of the left side list

        Steps:
        1. Deep copy the waiting queue of jobs
        2. Sort jobs descendingly in copy by rank
        3. Loop through all the available jobs in waiting queue
            a. Get all the cojobs that can fit inside the cluster. If none were
            found then go to the next job in the waiting queue.
            b. Order the possible partners by the wait_ordering heuristic.
            c. If a capable pair is found then remove both jobs from the waiting
            queue, set them up and append them to the deploy_list as a pair of
            [job, cojob].
        """

        ################################
        # Colocation with waiting jobs #
        ################################
        waiting_queue: List[Job] = deepcopy_list(self.cluster.waiting_queue)

        # Order waiting queue by needed cores starting with the lowest
        waiting_queue.sort(key=(lambda job: self.ranks[job.job_id]),
                           reverse=True)
        
        # Loop until waiting queue is empty
        while waiting_queue != []:

            # Get the job at the head
            job = self.pop(waiting_queue)
            
            # Create a dummy waiting queue without `job`
            wq = deepcopy_list(self.cluster.waiting_queue)
            wq.remove(job)

            # Filter out cojobs that can't fit into the execution list as pairs
            wq = list(filter(
                lambda cojob: 
                cojob.load.full_load_name in job.load.coloads and\
                2 * max(
                    self.cluster.half_node_cores(job),
                    self.cluster.half_node_cores(cojob)
                ) <= self.cluster.free_cores, wq
            ))

            # wq = list(filter(
            #     lambda cojob:
            #     self.pmatrix_get_speedup(job.job_id, cojob.job_id) >= 1, wq
            # ))

            # If empty, no pair can be made continue to the next job
            if wq == []:
                continue

            # Sort `wq` by the wait_ordering function
            wq.sort(key=(lambda x: self.wait_ordering(job, x, ll_avg_speedup)),
                    reverse=True)

            # The first is the best match
            best_match = wq[0]

            # Remove best match from waiting_queue
            waiting_queue.remove(best_match)

            # Remove jobs from waiting queue of cluster
            self.cluster.waiting_queue.remove(job)
            self.cluster.waiting_queue.remove(best_match)

            # Setup jobs before deploying
            needed_cores = max(
                    self.cluster.half_node_cores(job),
                    self.cluster.half_node_cores(best_match)
            )

            job.binded_cores = needed_cores
            best_match.binded_cores = needed_cores

            job.ratioed_remaining_time(best_match)
            best_match.ratioed_remaining_time(job)

            # Deploy them
            deploy_list.append([job, best_match])

            # Scheduler setup
            # total_deploying_cores += 2 * needed_cores
            self.cluster.free_cores -= 2 * needed_cores
            ll_avg_speedup = (ll_avg_speedup * ll_num + self.pair_avg_speedup(job, best_match)) / (ll_num + 1)
            ll_num += 1

            # Recalculate ranks
            self.update_ranks()

            # Logger cluster events update
            self.logger.cluster_events["deploying:wait-colocation"] += 1

        return ll_num, ll_avg_speedup

    def deploying_wait_compact(self, deploy_list, ll_num, ll_avg_speedup):

        #############################
        # Compact with waiting jobs #
        #############################
        waiting_queue = deepcopy_list(self.cluster.waiting_queue)
        
        # Order by the least amount of needed cores
        waiting_queue.sort(key=(lambda job: job.num_of_processes))

        while waiting_queue != []:

            job = self.pop(waiting_queue)

            if self.cluster.full_node_cores(job) <= int(self.cluster.free_cores):

                # Remove from cluster's waiting queue
                self.cluster.waiting_queue.remove(job)

                # Setup job
                job.binded_cores = self.cluster.full_node_cores(job)

                # Deploy job
                deploy_list.append([job])

                # Scheduler setup
                # total_deploying_cores += job.binded_cores
                self.cluster.free_cores -= job.binded_cores
                ll_avg_speedup = (ll_avg_speedup * ll_num + 1) / (ll_num + 1)
                ll_num += 1

                # Logger cluster events update
                self.logger.cluster_events["deploying:compact"] += 1

        return ll_num, ll_avg_speedup

    def deploying(self):
        """The specifications for the Balancer are as following:

        - If any job has 0 ranking then deploy it as compact.
        - Co-locate with executing half pairs: we try to find matches of the
          highest in terms of of needed cores half pairs with passable speedups.
        - Co-locate with waiting jobs: try to fit first the low in demand of
          needed cores jobs.
        - If not any of the above is true we deploy jobs in compact allocation
          policy. We start with jobs which have the lowest number of needed
          cores.
        - We will try to balance out the speedup between checkpoints by
          measuring how good a job in pairs is. It means how many pairs a job
          can construct that have an average speedup higher than 1. We want the
          jobs with highest ranking counter to be with us as much as possible.
        """

        # Important setups #

        # List of jobs to deploy
        deploy_list: List[List[Job]] = list()

        # Left list side average speedup
        ll_num, ll_avg_speedup = self.exec_avg_speedup()

        # Update ranks
        self.update_ranks()

        # Deploy any job that has 0 ranking in compact allocation policy
        for job_id in list(filter(lambda jid: 
                                  self.ranks[jid] == 0,
                                  self.ranks)):
            for job in self.cluster.waiting_queue:
                if job.job_id == job_id\
                        and self.cluster.full_node_cores(job) <= self.cluster.free_cores:
                            job.binded_cores = self.cluster.full_node_cores(job)
                            deploy_list.append([job])
                            self.cluster.free_cores -= job.binded_cores
                            # Remove from the waiting queue
                            self.cluster.waiting_queue.remove(job)
        
        # If some compact jobs where found then:
        # 1. Update the ranks
        # 2. Compute the new ll_avg_speedup
        if deploy_list != []:
            self.update_ranks()
            ll_avg_speedup = (ll_avg_speedup * ll_num + len(deploy_list)) / (ll_num + len(deploy_list))
            ll_num += len(deploy_list)

        # Co-location with execution half pairs
        ll_num, ll_avg_speedup = self.deploying_exec_pairs(deploy_list, 
                                                           ll_num,
                                                           ll_avg_speedup)

        # Co-location between waiting queue jobs
        ll_num, ll_avg_speedup = self.deploying_wait_pairs(deploy_list,
                                                           ll_num, 
                                                           ll_avg_speedup)

        # Compact allocation of waiting queue jobs
        # ll_num, ll_avg_speedup = self.deploying_wait_compact(deploy_list,
        #                                                      ll_num,
        #                                                      ll_avg_speedup)

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

