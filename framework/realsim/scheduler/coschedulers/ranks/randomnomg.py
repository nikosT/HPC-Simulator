from .random import RandomRanksCoscheduler


class RandomNoMgCoscheduler(RandomRanksCoscheduler):

    name = "Random Ranks Co-Scheduler Except MG"
    description = "Co-schedule every job other than the 'mg' benchmarks"


    def update_ranks(self):

        self.ranks = {job.job_id : 0 for job in self.cluster.waiting_queue}

        # Update ranks for each job
        for i, job in enumerate(self.cluster.waiting_queue):

            if "mg" in job.job_name:
                continue

            for co_job in self.cluster.waiting_queue[i+1:]:

                if "mg" in co_job.job_name:
                    continue

                job_speedup = self.heatmap[job.job_name][co_job.job_name]
                co_job_speedup = self.heatmap[co_job.job_name][job.job_name]

                if job_speedup is None or co_job_speedup is None:
                    continue

                avg_speedup = (job_speedup + co_job_speedup) / 2

                if avg_speedup > self.ranks_threshold:
                    self.ranks[job.job_id] += 1
                    self.ranks[co_job.job_id] += 1
