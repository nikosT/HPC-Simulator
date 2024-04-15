"""
Utility function used in various instances of the source code for Jobs inside
containers
"""

from .jobs import Job, EmptyJob


def deepcopy_list(jobs_list: list[Job] | list[list[Job]]):
    """
    Create and return a new list of jobs or lists of jobs.
    This function tries to fit all the oddities found in the simulation code
    and provide a single way of copying such lists
    ---
    Different lists of jobs are found in the simulation such as the waiting 
    queue and the execution list.
    """

    # Nothing to copy if the list is empty 
    # but return a new empty list for reference
    if jobs_list == []:
        return []

    # We will return this list
    new_list = list()

    # If the list is composed of Jobs
    if isinstance(jobs_list[0], Job):
        new_list = list(map(lambda job: job.deepcopy(), jobs_list))

    # If the list is composed of list of Jobs
    elif isinstance(jobs_list[0], list):
        for item in jobs_list:
            new_list.append(list(map(

                lambda job:
                job.deepcopy(), item
            
            )))

    else:
        raise Exception("The type of elements is neither Job nor List[Job]")

    # If everything turns out okay then return the new list
    return new_list

