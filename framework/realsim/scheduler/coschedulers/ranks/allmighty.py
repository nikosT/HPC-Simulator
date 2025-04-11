from abc import ABC
from .ranks import RanksCoscheduler
from numpy.random import seed, randint
from time import time_ns
from math import inf
import os
import sys
from realsim.jobs.utils import deepcopy_list
from itertools import combinations, chain
from numpy import mean
from math import ceil
import pandas as pd
import numpy as np
import json
import copy

sys.path.append(os.path.abspath(os.path.join(
    os.path.dirname(__file__), "../../../../"
)))

from realsim.jobs.jobs import Job
from realsim.cluster.host import Host
from realsim.scheduler.coschedulers.ranks.ranks import RanksCoscheduler
from realsim.jobs.jobs import JobCharacterization


class AlmightyCoscheduler(RanksCoscheduler, ABC):

    name = "Almighty Co-Scheduler"
    description = """Relational Co-scheduling with score in bulk assignment mode"""
    #queue_depth = 100
    def extract_weights(self, path, filters):
        if 'json' in path:
            # Read the JSON file
            with open(path, 'r') as file:
                json_data = json.load(file)
            
            # Parse loads and create initial DataFrame
            loads_list = [json.loads(load) for load in json_data["loads"]]
            df = pd.DataFrame(loads_list)
            result_df = df[['load_name', 'num_of_processes', 'compact_timelogs']].copy()
            result_df = result_df[~result_df['num_of_processes'].isin(filters)]
            result_df['compact_timelogs'] = result_df['compact_timelogs'].apply(lambda x: np.mean(x) if isinstance(x, list) and len(x) > 0 else x)
            result_df = result_df.drop_duplicates('load_name', keep='first')
            result_df = result_df.rename(columns={'num_of_processes': 'procs', 'compact_timelogs': 'time'})
            result_df.index = result_df['load_name'].tolist()
            result_df = result_df[['procs', 'time']]
            return result_df        
        else: # csv
            df = pd.read_csv(path)
            df = df.drop_duplicates('name_A', keep='first')
            df1 = df[['procs_A', 'compact_A']].rename(columns={'procs_A': 'procs', 'compact_A': 'time'})
            df1 = df1[~df1['procs'].isin(filters)]
            df1.index = df['name_A'].tolist()
        return df1
    def json_to_heatmap(self, file_path, filters=None):
        
        # Read the JSON file
        with open(file_path, 'r') as file:
            json_data = json.load(file)
            
        
        # Parse loads and create initial DataFrame
        loads_list = [json.loads(load) for load in json_data["loads"]]
        df = pd.DataFrame(loads_list)
        
        # Get the list of load names
        load_names = df['load_name'].tolist()
        load_names = [name for name in load_names if not any(str(pattern) in name for pattern in filters)]
        n = len(load_names)
        
        # Create an n×n DataFrame with load names as both index and columns
        result_df = pd.DataFrame(index=load_names, columns=load_names)
        
        # Fill each row with the compact_timelogs from the corresponding load
        for load_name in load_names:
            # Get the compact_timelogs for this load
            compact = np.median(df.loc[df['load_name'] == load_name, 'compact_timelogs'].iloc[0])
        
            for load_name2 in load_names:
                try:
                    _dict = df.loc[df['load_name'] == load_name, 'coscheduled_timelogs'].iloc[0]
                    result_df.loc[load_name, load_name2] = compact / np.median(_dict[load_name2][0])
                except:
                    result_df.loc[load_name, load_name2] = np.nan

        return result_df
    def calc_probability(self, data):
        """
        Takes a list of multiplicities [k1, k2, ..., ku] and returns a list of all possible
        pairs in C(n,2) with their counts, where n = k1 + k2 + ... + ku.
        
        Args:
            multiplicities (list): List of integers [k1, k2, ..., ku] representing counts of each type.
        
        Returns:
            list: List of tuples [((i, j), count), ...] for each pair {ai, aj} and its count.
        """
        multi = data['count'].tolist()
        weights = data['area'].tolist()
        
        u = len(multi)  # Number of unique elements
        result = []

        # Generate all possible pairs and their counts
        for i in range(u):
            # Same-type pairs {ai, ai}
            ki = multi[i]
            wi = weights[i]
            
            same_type_count = ((ki*wi) * ((ki*wi) - 1)) / 2 - wi*(wi-1) / 2
            result.append(((data.index[i], data.index[i]), same_type_count))
            
            # Mixed-type pairs {ai, aj} where j > i
            for j in range(i + 1, u):
                kj = multi[j]
                wj = weights[j]
                mixed_type_count = (ki*wi) * (kj*wj)
                result.append(((data.index[i], data.index[j]), mixed_type_count))

        n = sum(list(map(lambda x: x[1], result)))
        if n == 0:
            # return a zero dataframe
            return pd.DataFrame(0, index=data.index, columns=data.index)
        
        result = list(map(lambda x: (x[0], x[1]/n),result))

        # Create empty DataFrame
        df = pd.DataFrame(index=data.index, columns=data.index)
        
        # Fill the DataFrame
        for (row, col), value in result:
            df.at[row, col] = value
        
        # Fill NaN with 0 if needed
        df = df.infer_objects(copy=False).fillna(0)

        return df
    def calc_impact(self, df, probx):
        selected_indices = probx.index  # or ref_df['some_column'].values if indices are stored in a column
        selected_columns = probx.columns  # or ref_df['column_with_column_names'].values if stored in a column
        
        sub_df = df.loc[selected_indices, selected_columns]  
        sym_probx = probx + probx.T - np.diag(np.diag(probx))
        f_df = sub_df * sym_probx
        mean_df = pd.DataFrame(f_df.sum())
        
        # Initialize output DataFrame
        gain_df = pd.DataFrame(index=probx.index, columns=probx.columns)
        
        # Compute each cell conditionally
        for i in probx.index:
            for j in probx.columns:
                if df.loc[j,i] < df.loc[i,j]:
                    s_b = df.loc[j,i]
                    s_a = df.loc[i,j]
                else:
                    s_b = df.loc[i,j]
                    s_a = df.loc[j,i]               

                if mean_df.loc[j].values[0] != 0:
                    area = 2/s_a+(1/s_b-1/s_a)/mean_df.loc[j].values[0]
                else:
                    # Handle the case where mean_df is zero
                    # use s_b instead
                    area = 2/s_a+(1/s_b-1/s_a)/s_b
                gain_df.loc[i,j] = (area - 2) / 2

        mask = np.triu(np.ones_like(gain_df, dtype=bool), k=0)
        # Zero out the lower triangle (excluding diagonal)
        return gain_df.where(mask, 0)

    def __init__(self):
        super().__init__()
        self.queue_depth = 100
        self.score = 0
        path="/home/nikos/Desktop/ipdps2025/sim/lm-aris.compute-NAS.json"
        self.filters = [2025, 2048]
        self.dictionary = self.json_to_heatmap(path, self.filters)
        # Replace empty strings with NaN
        self.dictionary = self.dictionary.replace('', np.nan)
        self.weights = self.extract_weights(path, self.filters)
        self.weights['area'] = self.weights['procs']*self.weights['time'].round().astype(int)
        self.staging_list = []
        self.backfill_enabled = False

    def host_alloc_condition(self, hostname: str, job: Job) -> (float,float):
        """Condition on how to sort the hosts"""
        return super().host_alloc_condition(hostname, job)



    #def deploy(self) -> bool:

        def calc_score(waiting_queue: list[Job]) -> float:
            # Calculate the score of the waiting queue
            multi = pd.Series([job.job_name for job in waiting_queue]).value_counts().sort_index()
            data = pd.concat([self.weights, multi], axis=1, join='inner').fillna(0)
            probx = self.calc_probability(data)
            impactx = self.calc_impact(self.dictionary, probx)
            score = impactx*probx
            return float(score.sum().sum())

        deployed = False

        # Update the rank of each job before scheduling them
        # self.update_ranks()
        self.queue_depth = 100
        self.score = 0

        waiting_queue = deepcopy_list(self.cluster.waiting_queue[:self.queue_depth])
        waiting_queue.sort(key=lambda job: self.waiting_queue_reorder(job),
                           reverse=True)

        while waiting_queue != []:

            # calculate score
            score = calc_score(waiting_queue)

            # Remove from the waiting queue
            job = self.pop(waiting_queue)

            new_score = calc_score(waiting_queue)
            if new_score > score:
                alloc_policy = self.cluster.full_socket_allocation
            else:
                alloc_policy = self.cluster.half_socket_allocation

            # Colocate
            if self.allocation(job, alloc_policy):
                deployed = True
                self.after_deployment()
                break # that's the only addon
            else:
                break

        return deployed

    def deploy(self):
        # Get k jobs from the waiting list based on the number of idle cores in the system and put them in the staging list.
        # Set score = 0
        # Set spread-list = []
        # Set compact-list = []
        # Set leftovers = []
        # For each job in staging-list(k):
        # Pop job from staging-list
        # If new score >= score:
        # If job can fit as spread add it to spread-list
        # If no: add it to leftovers list and continue the iteration
        # If new score < score:
        # If job can fit as compact add it to compact-list
        # If no: add it to leftovers list and continue the iteration
        # Sort spread-list by favor
        # For each job in spread-list (which is <=k) deploy it as spread in sequential order. The hosts are given in beneficial order [like util] (or in sequential order).
        # For each job in compact-list (which is <=k and also spread-list + compact-list = staging-list(k)), deploy it as compact.
        # Set leftovers as the new staging-list. The new staging list now contains <=k jobs. It will be equal only if no job could make it to allocation previously.
        # Go to 1 until leftovers (new staging-list) is empty.
        def calc_score(waiting_queue: list[Job]) -> float:
            # Calculate the score of the waiting queue
            multi = pd.Series([job.job_name for job in waiting_queue]).value_counts().sort_index()
            data = pd.concat([self.weights, multi], axis=1, join='inner').fillna(0)
            probx = self.calc_probability(data)
            impactx = self.calc_impact(self.dictionary, probx)
            score = impactx*probx
            return float(score.sum().sum())
        
        print('start')

        if self.staging_list == []:

            waiting_queue = copy.copy(self.cluster.waiting_queue[:self.queue_depth])
            waiting_queue.sort(key=lambda job: self.waiting_queue_reorder(job),
                            reverse=True)
            print("waiting_queue", waiting_queue)
        
            # PHASE 1: Keep only the first k jobs that can be allocated

            # get k jobs from the waiting list based on the number of idle cores in the system and put them in the staging list
            available_cores = self.cluster.get_idle_cores()
            requested_cores = 0

            for job in waiting_queue:
                requested_cores += job.num_of_processes
                if requested_cores <= available_cores:
                    self.staging_list.append(job)
                else:
                    break

        # PHASE 2: Classify the jobs in the staging list by their allocation policy (tagging)

        # staging_list now contains all jobs in order that theoritically can be allocated in terms of cores
        # but not in terms of allocation policy
        # internal variables
        score = 0
        compact_list = []
        spread_list = []

        # the loop will simulate a dry run of the allocations
        # so leftover jobs will be added to the leftovers list
        # and will be used as the new staging list
        # after the actual allocation is completed
        leftovers = []

        temp_self = copy.deepcopy(self)

        while self.staging_list != []:
            job = self.pop(self.staging_list)

            #new_score = calc_score(self.staging_list)
            new_score = 10

            if new_score >= score:
                # Check if the job can be allocated in a spread manner
                if temp_self.allocation(job, self.cluster.half_socket_allocation):
                    spread_list.append(job)
                else:
                    leftovers.append(job)
            else:
                if temp_self.allocation(job, self.cluster.full_socket_allocation):
                    compact_list.append(job)
                else:
                    leftovers.append(job)
  
        # PHASE 3: Allocate the jobs

        # Sort the spread list by favor
        #spread_list.sort(key=lambda job: self.waiting_queue_reorder(job),
        #                 reverse=True)
        for job in spread_list:
            self.allocation(job, self.cluster.half_socket_allocation)
        
        for job in compact_list:
            self.allocation(job, self.cluster.full_socket_allocation)

        # PHASE 4: set the leftovers as the new staging list
        self.staging_list = leftovers

        print("spread_list", spread_list)
        print("compact_list", compact_list)
        print("leftovers", leftovers)
        print(self.staging_list)
        print(self.cluster.waiting_queue)
        print("Execution list", self.cluster.execution_list)

        if spread_list == [] and compact_list == []:
            return False
        
        self.after_deployment()       
        return True

    def backfill(self) -> bool:
        return False
        return super().backfill()

    def after_deployment(self, *args):
        return super().after_deployment(*args)
