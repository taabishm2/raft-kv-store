""" Main Raft class.
"""

from .log_manager import *
from .election import *
from .transport import *
from .transport import *

class RaftNode:
    def __init__(self, name, peers):
        self.name = name

        self.current_term = 0

        self.log_manager = LogManager()
        self.transport = Transport(peers, self.log_manager)
        self.election = Election(self.current_term, transport=self.transport)

    def serve_put_request(self, key, value):
        """
        Service the put request from client.

        #TODO: In the first version, leader simply appends to its own log and returns.
        """
        log_item = LogEntry(self.current_term, key, value)
        self.log_manager.append(log_item)

        return True

