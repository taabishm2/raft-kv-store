import time
from threading import Lock
class Stats:

    def __init__(self):
        self.lock = Lock()

        self.commit_latency = []        # (num_peers, latency): Latency in performing commits for PUT requests
        self.kv_request_list = []       # (request_name, time): List of KV RPC calls received (for throughput)
        self.raft_request_list = []     # (request_name, time): List of RAFT RPC received (for throughput)

    def add_commit_latency(self, num_peers, latency):
        with self.lock:
            self.commit_latency.append((num_peers, latency))

    def add_kv_request(self, request_name):
        with self.lock:
            self.kv_request_list.append((time.time(), request_name))

    def add_raft_request(self, request_name):
        with self.lock:
            self.raft_request_list.append((time.time(), request_name))

stats = Stats()

