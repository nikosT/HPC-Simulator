from concurrent.futures import ProcessPoolExecutor, as_completed
import os
import sys
from time import time
from datetime import timedelta

sys.path.append(os.path.abspath(
    os.path.join(os.path.dirname(__file__), "..")
))

from batch.batch_utils import BatchCreator
from run_utils import simulation

def main():
    # Create batch ranks
    batch_creator = BatchCreator(sys.argv[1])
    batch_creator.create_ranks()

    start_time = time()
    print("Starting simulations...")

    # Create the multiprocessing executor
    with ProcessPoolExecutor() as executor:
        # Submit all jobs to the executor and store futures
        futures = {executor.submit(simulation, sim_batch): sim_batch for sim_batch in batch_creator.ranks}

        # Monitor and log progress as futures complete
        for future in as_completed(futures):
            sim_batch = futures[future]
            try:
                result = future.result()  # Get the result of the simulation
                print(f"Simulation completed for batch {sim_batch}: {result}")
            except Exception as e:
                print(f"Simulation failed for batch {sim_batch}: {e}")

    end_time = time()
    print(f"All simulations completed in {timedelta(seconds=end_time - start_time)}")

if __name__ == "__main__":
    main()

