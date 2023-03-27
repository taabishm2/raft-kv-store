import threading
from math import factorial
from collections import deque
from concurrent import futures
from raft.node import RaftNode

import grpc
import kvstore_pb2
import kvstore_pb2_grpc


# Global variables
DATABASE_DICT = dict()

LOCK = threading.Lock()

class KVStoreServicer(kvstore_pb2_grpc.KVStoreServicer):
    def __init__(self, name):
        super().__init__()
        self.raft_node = RaftNode(name)

    def Put(self, request, context):
        global DATABASE_DICT

        print(f'put {request} {request.value}')

        # Serve put request.
        self.raft_node.serve_put_request(request.key, request.value)

        # TODO: Do you need just one type of lock or two types (for setnum and fact)?
        # read, write locks for database, ig.
        with LOCK:
            DATABASE_DICT[request.key] = request.value

        return kvstore_pb2.PutResponse(error="")

    def Get(self, request, context):
        global DATABASE_DICT

        key, val = request.key, None

        with LOCK:
            if not key in DATABASE_DICT:
                return kvstore_pb2.GetResponse(value = "", hit=False, error=f"Key:{key} not found.")
            val = DATABASE_DICT[key]

        print(f'get {key} {val}')
            
        return kvstore_pb2.GetResponse(value=str(val), hit=True)

def run_server():
    grpc_server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    
    servicer = KVStoreServicer("leader")
    kvstore_pb2_grpc.add_KVStoreServicer_to_server(servicer, grpc_server)
    grpc_server.add_insecure_port('[::]:5440')
    
    print("Server listening on port:5440")
    grpc_server.start()
    grpc_server.wait_for_termination()
    print("Server terminated")

if __name__ == '__main__':
    run_server()