import threading
from concurrent import futures
# from raft.node import RaftNode

import sys
import grpc
import threading

sys.path.append('../')

from concurrent import futures
import kvstore_pb2
import kvstore_pb2_grpc

# sys.path.append('./raft')

# Global variables
DATABASE_DICT = dict()
LOCK = threading.Lock()


class KVStoreServicer(kvstore_pb2_grpc.KVStoreServicer):
    def __init__(self):
        super().__init__()
        # self.raft_node = RaftNode(name)

    def Put(self, request, context):
        global DATABASE_DICT
        print(f"[LOG]: PUT {request.key}:{request.value}")

        print(f'put {request} {request.value}')

        # Serve put request.
        # self.raft_node.serve_put_request(request.key, request.value)

        # TODO: Do you need just one type of lock or two types (for setnum and fact)?
        # read, write locks for database, ig.
        with LOCK:
            DATABASE_DICT[request.key] = request.value

        return kvstore_pb2.PutResponse(error="")

    def Get(self, request, context):
        global DATABASE_DICT
        print(f"[LOG]: GET {request.key}")

        with LOCK:
            if request.key in DATABASE_DICT:
                return kvstore_pb2.GetResponse(key_exists=True, key=request.key, value=DATABASE_DICT[request.key])
            return kvstore_pb2.GetResponse(key_exists=False, key=request.key, value=None)


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    kvstore_pb2_grpc.add_KVStoreServicer_to_server(KVStoreServicer(), server)

    server.add_insecure_port('[::]:5440')
    print("Server listening on port:5440")

    f = open("/raft-kv-store/server/raft/server.out", "w")
    f.write("Server started\n")
    server.start()
    server.wait_for_termination()

    f.write("Server terminated")
    f.close()

    print("Server terminated")


if __name__ == '__main__':
    serve()
