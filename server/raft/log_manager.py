import os
import pickle
import datetime;

from threading import Lock

import raft_pb2
import raft_pb2_grpc

RAFT_BASE_DIR = './raft-kv'
RAFT_LOG_PATH = RAFT_BASE_DIR + '/log'

class RaftState:
    def __init__(self):
        self.current_term = 0
        self.commit_index = 0  # Index of highest log entry known to be committed
        self.last_applied = 0  # Index of highest log entry applied to state machine
        self.voted_for_ip = ""
        self.state = {}
        self.entries = list()

######################### LogEntry class ##########################

class LogEntry:
    def __init__(self, term, cmd_key, cmd_val):
        self.term = term
        self.cmd_key = cmd_key
        self.cmd_val = cmd_val

def to_grpc_log_entry(entry):
    log_entry = raft_pb2.LogEntry(
            log_term = int(entry.term),
            command = raft_pb2.WriteCommand(
                key = entry.cmd_key,
                value = entry.cmd_val,
            )
        )
    
    return log_entry

def from_grpc_log_entry(entry):
    write_command = entry.command
    log_entry = LogEntry(entry.log_term,
        write_command.key, write_command.value)
    
    return log_entry

######################### LogManager class ##########################

class LogManager:
    def __init__(self, name):
        self.name = name

        self.current_term = 1
        self.commit_index = 0  # Index of highest log entry known to be committed
        self.last_applied = 0  # Index of highest log entry applied to state machine
        self.voted_for_ip = ""
        self.entries = list()
        self.lock = Lock()
        self.load_entries() # Load entries from stable store to memory.

    def get_log_entry(self, key, value):
        return LogEntry(self.current_term, key, value)

    def get_current_term(self):
        return self.current_term

    def get_name(self):
        return self.name

    def load_entries(self):
        if not os.path.exists(RAFT_BASE_DIR):
            os.makedirs(RAFT_BASE_DIR) # Create empty 

        if not os.path.exists(RAFT_LOG_PATH):
            self.flush_log_to_disk()  # Create empty log file

        file = open(RAFT_LOG_PATH, 'rb')
        self.entries = pickle.load(file)
        num_entries = len(self.entries)
        self.output_log(f'Loaded {num_entries} entries')
        file.close()

    def append(self, log_entry):
        """
        append entry to the node's replicated log. This method is to be used by the leader
        to populate its own log. Returns last log index if successful.
        """
        with self.lock:
            self.entries.append(log_entry)
            self.flush_log_to_disk()
            
            return len(self.entries)

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
        if len(self.entries) == 0 or self.commit_index + 1 >= len(self.entries):
            return None
        return self.entries[self.commit_index + 1]

    def commit_log_entry(self):
        """
        Mark the earliest uncommitted log entry as committed (increment commit_index)
        """
        if self.get_earliest_uncommitted() is None:
            raise ValueError("No valid log entry present to commit")
        self.commit_index += 1

    def get_earliest_unapplied(self):
        """
        Get the earliest unapplied log entry or None if it doesn't exist
        """
        if len(self.entries) == 0 or self.last_applied + 1 >= len(self.entries):
            return None
        return self.entries[self.last_applied + 1]

    def mark_log_applied(self):
        """
        Mark the earliest unapplied log entry as applied (increment last_applied)
        """
        if self.get_earliest_unapplied() is None:
            raise ValueError("No valid log entry present to apply")
        self.last_applied += 1

    def flush_log_to_disk(self):
        log_file = open(RAFT_LOG_PATH, 'wb')
        pickle.dump(self.entries, log_file)
        log_file.close()

    def output_log(self, log):
        print(f"[LOG]: {datetime.datetime.now()} {log}")
