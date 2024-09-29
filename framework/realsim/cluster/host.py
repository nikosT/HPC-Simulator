from procset import ProcSet

class Host:

    def __init__(self,
                 socket_conf: tuple[int], 
                 first_core_id: int):
        
        self.socket_conf = socket_conf
        self.sockets: list[ProcSet] = list()

        # Define sockets
        _count = first_core_id
        for cores in socket_conf:
            self.sockets.append(ProcSet((_count, _count + cores - 1)))
            _count += cores

        # Get references of the jobs running on the host
        self.jobs: dict[str, list[ProcSet]] = dict()

# Alias for Host class
Node = Host
