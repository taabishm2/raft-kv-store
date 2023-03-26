import sys
import grpc
import threading

sys.path.append('../')

from concurrent import futures
import kvstore_pb2
import kvstore_pb2_grpc

# Global variables
DATABASE_DICT = dict()
LOCK = threading.Lock()


class KVStoreServicer(kvstore_pb2_grpc.KVStoreServicer):
    def Put(self, request, context):
        global DATABASE_DICT
        print(f"[LOG]: PUT {request.key}:{request.value}")

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

    f = open("/raft-kv-store/raft/server.out", "w")
    f.write("Server started\n")
    server.start()
    server.wait_for_termination()

    f.write("Server terminated")
    f.close()

    print("Server terminated")


if __name__ == '__main__':
    serve()
