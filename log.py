import os
import pickle

RAFT_BASE_DIR = '/raft-kv'
RAFT_LOG_PATH = RAFT_BASE_DIR + '/log'


class LogEntry:
    def __init__(self, term, cmd_key, cmd_val):
        self.term = term
        self.cmd_key = cmd_key
        self.cmd_val = cmd_val


class Log:

    def __init__(self):
        self.commit_index = 0  # Index of highest log entry known to be committed
        self.last_applied = 0  # Index of highest log entry applied to state machine
        self.entries = list()

        if not os.path.exists(RAFT_LOG_PATH):
            self.flush_log_to_disk()  # Create empty log file

        file = open(RAFT_LOG_PATH, 'rb')
        self.entries = pickle.load(file)
        file.close()

    def overwrite(self, start_index, log_entry_list, previous_term):
        """
        Overwrite entries of replicated log starting from (and including) `start_index` with `log_entry_list`
        Returns false if previous log index term doesn't match and true if overwrite successful
        """
        if start_index > len(self.entries) or \
                (start_index > 0 and (self.entries[start_index - 1].term != previous_term)):
            return False

        self.entries[start_index:] = log_entry_list
        self.flush_log_to_disk()
        return True

    def get_earliest_uncommitted(self):
        if len(self.entries) == 0 or self.commit_index + 1 >= len(self.entries):
            return None
        return self.entries[self.commit_index + 1]

    def commit_log_entry(self):
        if self.get_earliest_uncommitted() is None:
            raise ValueError("No valid log entry present to commit")
        self.commit_index += 1

    def get_earliest_unapplied(self):
        if len(self.entries) == 0 or self.last_applied + 1 >= len(self.entries):
            return None
        return self.entries[self.last_applied + 1]

    def mark_log_applied(self):
        if self.get_earliest_unapplied() is None:
            raise ValueError("No valid log entry present to apply")
        self.last_applied += 1

    def flush_log_to_disk(self):
        log_file = open(RAFT_LOG_PATH, 'wb')
        pickle.dump(self.entries, log_file)
        log_file.close()
