from .election import *
from .transport import transport


class RaftNode:

    def serve_put_request(self, key, value):
        """Returns tuple: (success (bool), error message) TODO: Redirect to leader node."""
        if not globals.state == NodeRole.Leader:
            return False, "node not leader"

        log_item = LogEntry(globals.current_term, key, value)
        index = log_manager.append(log_item)
        transport.append_entry_to_peers(log_item, index)

        return True, ""


# Create raft_node singleton
raft_node = RaftNode()
