"""
Utility function used in various instances of the source code for Jobs inside
containers
"""

from typing import List
from .jobs import Job, EmptyJob


def deepcopy_list(jobs_list: List[Job] | List[List[Job]]):
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

    for item in jobs_list:
        if isinstance(item, Job):
            # Get reference to a true new copy
            new_list.append( item.deepcopy() )
        elif type(item) == list:
            if len(item) == 1 and isinstance(item[0], Job):
                new_item = [ item[0].deepcopy() ]
                new_list.append(new_item)
            elif len(item) == 2 and isinstance(item[0], Job) and isinstance(item[1], Job):
                new_item = [item[0].deepcopy(), item[1].deepcopy()]
                new_list.append(new_item)
            elif len(item) > 2:
                new_item = list()
                for job in item:
                    if not isinstance(job, Job):
                        raise Exception("Not a job")
                    new_item.append( job.deepcopy() )
                new_list.append(new_item)
            else:
                raise Exception("More items inside the list or " + 
                                "the type was wrong")
        else:
            raise Exception("The type of elements is neither Job nor List[Job]")

    # If everything turns out okay then return the new list
    return new_list

