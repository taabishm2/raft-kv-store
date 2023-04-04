import time

from .election import *
from .transport import transport
from .log_manager import *
from .stats import stats
from time import sleep


class RaftNode:

    def serve_put_request(self, key, value):
        """Returns tuple: (success (bool), error message) TODO: Redirect to leader node."""
        if globals.is_unresponsive:
            log_me("Am going to sleepzzzz")
            while True:
                sleep(1)

        log_item = LogEntry(globals.current_term, key, value)
        index = log_manager.append(log_item)

        t1 = time.time()
        is_success_on_majority = transport.append_entry_to_peers(log_item, index)
        stats.add_commit_latency(transport.peer_ips, time.time() - t1, is_success_on_majority)

        if is_success_on_majority:
            globals.set_commit_index(index)

        return is_success_on_majority, ""


# Create raft_node singleton
raft_node = RaftNode()
