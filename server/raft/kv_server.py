import sys
import threading
import time
from concurrent import futures

import grpc

from .config import globals, NodeRole
from .log_manager import *
from .node import raft_node
from .utils import *
from .stats import stats

sys.path.append('../../')
import kvstore_pb2
import kvstore_pb2_grpc

from pymemcache.client import base


class KVStoreServicer(kvstore_pb2_grpc.KVStoreServicer):
    def __init__(self):
        super().__init__()
        self.kv_store_lock = threading.Lock()
        self.client = base.Client(('memcached', 11211))

        self.sync_kv_store_with_logs()

    def sync_kv_store_with_logs(self):
        for entry in log_manager.entries[globals.lastApplied:(globals.commitIndex+1)]:
            with self.kv_store_lock:
                self.client.set(entry.cmd_key, entry.cmd_val)
                globals.set_last_applied(globals.commitIndex)

    def Put(self, request, context):
        stats.add_kv_request("PUT")
        log_me(f"Put {request.key} {request.value}")

        if not globals.state == NodeRole.Leader:
            log_me("Redirecting to leader: " + str(globals.leader_name))
            return kvstore_pb2.PutResponse(error="Redirect", is_redirect=True, redirect_server=globals.leader_name)

        is_consensus, error = raft_node.serve_put_request(request.key, request.value)

        if is_consensus:
            # This can be done in a separate thread.
            self.sync_kv_store_with_logs()
        else:
            error = "No consensus was reached. Try again."

        log_me(f"Consensus {is_consensus}, error {error}")
        return kvstore_pb2.PutResponse(error=error)

    def Get(self, request, context):
        stats.add_kv_request("GET")
        log_me(f"Get {request.key}")

        if not globals.state == NodeRole.Leader:
            log_me("Redirecting to leader: " + str(globals.leader_name))
            return kvstore_pb2.GetResponse(key_exists=False, is_redirect=True, redirect_server=globals.leader_name)

        with self.kv_store_lock:
            cached_val = self.client.get(request.key)
            return kvstore_pb2.GetResponse(key_exists=cached_val is not None,
                                           key=request.key, value=cached_val)


def main(port=5440):
    grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    kvstore_pb2_grpc.add_KVStoreServicer_to_server(KVStoreServicer(), grpc_server)
    grpc_server.add_insecure_port(f'[::]:{port}')

    log_me(f"{globals.name} KV-server listening on: {port}")
    grpc_server.start()
    grpc_server.wait_for_termination()
    log_me(f"{globals.name} KV-server terminated")


if __name__ == '__main__':
    main()
