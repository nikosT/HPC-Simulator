# Jobs

The classes **Job** and **EmptyJob** are defined in this package. An instance of
class **Job** encapsulates an instance of class **Load** and provides additional
information, about resource usage for the simulation. Class **EmptyJob** is
basically a subclass of class **Job** for job instances that have finished
execution. It is necessary because information about the binded cores of a job
is stored.

## User and Developer Guide

These classes are not used as is in the simulation framework. They are the
building blocks.

The next diagram shows the class hierarchy of the package.

```mermaid
classDiagram
    direction LR

    Job <|-- EmptyJob

    class Job{
        +Load load
        +int job_id
        +str job_name
        +int num_of_processes
        +float remaining_time
        +float queued_time
        +float wall_time
        +int binded_cores
        +float speedup
        +int gave_position
        +__init__(Optional[Load], int, str, int, float, float, int): None
        +__eq__(Job job): bool
        +__repr__(): str
        +get_speedup(Job cojob): float
        +get_overall_speedup(): float
        +get_max_speedup(): float
        +ratioed_remaining_time(Job cojob): None
        +deepcopy(): Job
    }

    class EmptyJob{
        +__init__(Job job): None
        +__repr__(): str
        +deepcopy(): EmptyJob
    }
```
