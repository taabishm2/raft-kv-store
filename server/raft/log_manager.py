import os
import pickle
from threading import Lock

from .config import globals
from .utils import *

RAFT_BASE_DIR = './logs/logcache'
RAFT_LOG_PATH = RAFT_BASE_DIR + '/stable_log'


class LogEntry:
    def __init__(self, term, cmd_key, cmd_val):
        self.term = term
        self.cmd_key = cmd_key
        self.cmd_val = cmd_val


class LogManager:
    def __init__(self):
        self.lock = Lock()
        self.entries = []
        self.load_entries()  # Load entries from stable store to memory.

    def load_entries(self):
        if not os.path.exists(RAFT_BASE_DIR):
            os.makedirs(RAFT_BASE_DIR)  # Create empty

        if not os.path.exists(RAFT_LOG_PATH):
            self.flush_log_to_disk()  # Create empty log file

        file = open(RAFT_LOG_PATH, 'rb')
        self.entries = pickle.load(file)
        num_entries = len(self.entries)
        log_me(f'Loaded {num_entries} log entries from disk')
        file.close()

    def append(self, log_entry):
        """
        append entry to the node's replicated log. This method is to be used by the leader
        to populate its own log. Returns last log index (length - 1) if successful.
        """
        with self.lock:
            self.entries.append(log_entry)
            self.flush_log_to_disk()

            return len(self.entries) - 1

    def overwrite(self, start_index, log_entry_list, previous_term):
        """
        Overwrite entries of replicated log starting from (and including) `start_index` with `log_entry_list`
        Returns false if previous log index term doesn't match and true if overwrite successful
        """
        if start_index > len(self.entries) or \
                (start_index > 0 and (self.entries[start_index - 1].term != previous_term)):
            return False

        with self.lock:
            self.entries[start_index:] = log_entry_list
            self.flush_log_to_disk()

        return True

    def is_this_log_older(self, current_term, other_log_term, other_log_len):
        """
        Used to decide whether to vote for a candidate
        """
        return current_term < other_log_term or (current_term == other_log_len and len(self.entries) < other_log_len)

    def get_log_at_index(self, index):
        """
        Get the earliest uncommitted log entry or None if it doesn't exist
        """
        if index >= len(self.entries):
            return None
        return self.entries[index]

    def get_earliest_uncommitted(self):
        """
        Get the earliest uncommitted log entry or None if it doesn't exist
        """
        if len(self.entries) == 0 or globals.commit_index + 1 >= len(self.entries):
            return None
        return self.entries[globals.commit_index + 1]

    def commit_log_entry(self):
        """
        Mark the earliest uncommitted log entry as committed (increment commit_index)
        """
        if self.get_earliest_uncommitted() is None:
            raise ValueError("No valid log entry present to commit")
        globals.commit_index += 1

    def get_earliest_unapplied(self):
        """
        Get the earliest unapplied log entry or None if it doesn't exist
        """
        if len(self.entries) == 0 or globals.last_applied + 1 >= len(self.entries):
            return None
        return self.entries[globals.last_applied + 1]

    def mark_log_applied(self):
        """
        Mark the earliest unapplied log entry as applied (increment last_applied)
        """
        if self.get_earliest_unapplied() is None:
            raise ValueError("No valid log entry present to apply")
        globals.last_applied += 1

    def flush_log_to_disk(self):
        log_file = open(RAFT_LOG_PATH, 'wb')
        pickle.dump(self.entries, log_file)
        log_file.close()

    def reset_timeout(self):
        '''
        reset the election timeout after receiving heartbeat
        from the leader
        '''
        # TODO Fix this
        # TODO: This can also be in config.py
        # randomtimeout function is moved to utils.py
        # self.election_time = time.time() + config.random_timeout()

    def get_last_index(self):
        return len(self.entries)

    def get_latest_term(self):
        if not self.entries: return 0
        return self.entries[-1].term


log_manager = LogManager()
