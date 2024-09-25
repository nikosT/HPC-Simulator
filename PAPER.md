# What is the simulator

The simulator we created is a statistical representation of the execution of a
workload under resource limits of an HPC system with different scheduling
algorithms. The workload is built upon real experiment logs under a specific HPC
infrastructure. For the simulator to produce reliable results the logs that
build the workload and the specifications of the cluster must be from the same
HPC system. The schedulers are algorithms for job placement and resource
handlers under the restraints and limitation of said simulated cluster. The main
attraction of the simulator is the ability for rapid development of scheduling
algorithms.

# How is it different from others

The computation and communication of each job are not simulated. It works under 
the premise that these values are obtained by the real experiment logs and these 
reflect the infrastructure they ran on. What is simulated is the execution of
the workload under different scheduling algorithms by varying conditions and
limitations.

Another major difference is that the simulator is employed with a model for
co-allocated execution. A user can also provide their own analytical or
stochastic model with pretty ease.

# What are the advantages

1. Simplicity in the design architecture leads to better understanding,
   maintainability and extensibility.
2. A distributed MVC architecture which is easy to be expanded by other
   researchers. It helps in rapid development and testing for simulation
   algorithms.
3. Optimized to produce fast results for large simulations. Even a low
   performance computer can run a simulation of many jobs.
4. The simulations are scalable. A simulation is roughly represented as a
   workload and a scheduler. The framework provides the ability to distribute
   the work as workloads and one scheduler (many-to-1), a workload with many 
   schedulers (1-to-many) and workloads with many schedulers (many-to-many).
5. The simulator has a GUI for ease of use.
5. It provides extensive reports and plots for a simulation without additional
   coding needed.
6. It is a standalone software solution for HPC simulations. There are no third
   party applications required.


# What are the disadvatages

1. No real computation and communication simulations for the jobs.
2. Doesn't fully support artificial workloads. The workloads must be built upon
   real experiment logs that reflect the HPC system that ran on.
