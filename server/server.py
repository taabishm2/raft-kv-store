import os
import sys
import threading
from concurrent import futures

sys.path.append('../')

import grpc
import kvstore_pb2
import kvstore_pb2_grpc

from raft.node import RaftNode

# Global variables
DATABASE_DICT = dict()
LOCK = threading.Lock()

class KVStoreServicer(kvstore_pb2_grpc.KVStoreServicer):
    def __init__(self):
        super().__init__()

        # Initialize raft node.
        self.raft_node = RaftNode()

    def Put(self, request, context):
        global DATABASE_DICT
        print(f"[LOG]: PUT {request.key}:{request.value}")

        # TODO: Do you need just one type of lock or two types (for setnum and fact)?
        # read, write locks for database, ig.
        with LOCK:
            DATABASE_DICT[request.key] = request.value

        # Serve put request using consensus.
        _, errMsg = self.raft_node.serve_put_request(request.key, request.value)

        return kvstore_pb2.PutResponse(error=errMsg)

    def Get(self, request, context):
        global DATABASE_DICT

        with LOCK:
            if request.key in DATABASE_DICT:
                resp = DATABASE_DICT[request.key]
                print(f"[LOG]: GET {request.key}, return {resp}")
                return kvstore_pb2.GetResponse(key_exists=True, key=request.key, value=resp)

            print(f"[LOG]: GET {request.key}, return None")
            return kvstore_pb2.GetResponse(key_exists=False, key=request.key, value=None)

def serve():
    grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    kvstore_pb2_grpc.add_KVStoreServicer_to_server(
        KVStoreServicer(), grpc_server)

    grpc_server.add_insecure_port('[::]:5440')
    print("Server listening on port:5440")

    f = open("/raft-kv-store/server/raft/server.out", "w")
    f.write("Server started\n")
    grpc_server.start()
    grpc_server.wait_for_termination()

    f.write("Server terminated")
    f.close()

    print("Server terminated")


if __name__ == '__main__':
    serve()
