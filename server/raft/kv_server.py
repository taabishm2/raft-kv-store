import sys
import threading
from concurrent import futures

import grpc

from node import raft_node
from utils import *

sys.path.append('../../')
import kvstore_pb2
import kvstore_pb2_grpc


class KVStoreServicer(kvstore_pb2_grpc.KVStoreServicer):
    def __init__(self):
        super().__init__()
        self.kv_store = dict()
        self.kv_store_lock = threading.Lock()

    @debug
    def Put(self, request, context):
        is_consensus, error = raft_node.serve_put_request(request.key, request.value)

        with self.kv_store_lock:
            if is_consensus:
                self.kv_store[request.key] = request.value

        return kvstore_pb2.PutResponse(error=error)

    @debug
    def Get(self, request, context):
        with self.kv_store_lock:
            return kvstore_pb2.GetResponse(key_exists=request.key in self.kv_store,
                                           key=request.key, value=self.kv_store.get(request.key))


def serve():
    grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    kvstore_pb2_grpc.add_KVStoreServicer_to_server(KVStoreServicer(), grpc_server)
    grpc_server.add_insecure_port('[::]:5440')

    print(f"{raft_node.name} listening on port:5440")
    grpc_server.start()
    grpc_server.wait_for_termination()
    print(f"{raft_node.name} terminated")


if __name__ == '__main__':
    serve()
