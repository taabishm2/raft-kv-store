import os

from .log_manager import *
from .election import *
from .transport import *
from .transport import *


class NodeRole(enum.Enum):
    Follower = 1
    Candidate = 2
    Leader = 3


class RaftNode:
    def __init__(self):
        self.name = os.environ['NAME']
        # TODO: Delete IS_LEADER and use leader election instead (always begin as Follower)
        self.state = NodeRole.Leader if os.environ['IS_LEADER'] else self.state = NodeRole.Follower

        self.current_term = 0
        self.__log_manager = LogManager(self.name)
        self.__transport = Transport(self.__log_manager)
        self.__election = Election(self.current_term, self.__log_manager, transport=self.__transport)

        print(f"{self.name} node up.\nCurrent term: {self.current_term}\nPeers: {peers}")

    # TODO what if you dont hear back from majority after a long period
    def serve_put_request(self, key, value):
        """Returns tuple: (success (bool), error message)"""
        if not self.state == NodeRole.Leader:
            # TODO: Redirect to leader node.
            return False, "node not leader"

        log_item = LogEntry(self.current_term, key, value)

        # Append log item to current node's log.
        index = self.__log_manager.append(log_item)
        self.__log_manager.output_log("Appended entry to leader's log.")

        # Push append entries to other peers.
        self.__transport.append_entry_to_peers(log_item, index)

        return True, ""


# Create raft_node singleton
raft_node = RaftNode()
