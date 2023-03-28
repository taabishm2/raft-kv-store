""" Main Raft class.
"""

import enum
from .log_manager import *
from .election import *
from .grpc_transport import *
from .transport import *

# Using enum class create enumerations
class NodeRole(enum.Enum):
    Follower = 1
    Candidate = 2
    Leader = 3

class RaftNode:
    def __init__(self, name, peers):
        self.name = name

        self.current_term = 0
        self.role = NodeRole.Follower

        self.log_manager = LogManager()
        self.transport = Transport()
        self.election = Election(
            node=self, transport=self.transport, store=self.__store, queue=self.q)

    def serve_put_request(self, key, value):
        """
        Service the put request from client.

        #TODO: In the first version, leader simply appends to its own log and returns.
        """
        log_item = LogEntry(self.current_term, key, value)
        self.log_manager.append(log_item)

        return True

