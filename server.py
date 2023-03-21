import threading
from math import factorial
from collections import deque
from concurrent import futures

import grpc
import numstore_pb2
import numstore_pb2_grpc


# Global variables
VALUES_DICT = dict()
VALUE_SUM = 0

FACTORIAL_CACHE = dict()
FACTORIAL_CACHE_MAX_SIZE = 10
FACTORIAL_CACHE_QUEUE = deque()

LOCK = threading.Lock()


class NumStoreServicer(numstore_pb2_grpc.NumStoreServicer):

    def SetNum(self, request, context):
        global VALUES_DICT, VALUE_SUM

        # TODO: Do you need just one type of lock or two types (for setnum and fact)?
        with LOCK:
            if request.key in VALUES_DICT:
                VALUE_SUM -= VALUES_DICT[request.key]

            VALUES_DICT[request.key] = request.value
            VALUE_SUM += request.value

            return numstore_pb2.SetNumResponse(total=VALUE_SUM)

    def Fact(self, request, context):
        global VALUES_DICT, FACTORIAL_CACHE

        key, val = request.key, None
        factorial_val = None

        with LOCK:
            if not key in VALUES_DICT:
                return numstore_pb2.FactResponse(hit=False, error=f"Key:{key} not found.")
            val = VALUES_DICT[key]

            if key in FACTORIAL_CACHE:
                factorial_val = FACTORIAL_CACHE[key]

        if factorial_val is None:
            factorial_val = factorial(val)

        with LOCK:
            update_factorial_cache(key, factorial_val)
            
        return numstore_pb2.FactResponse(value=factorial_val, hit=True)


# Cache eviction policy: LRU
def update_factorial_cache(key, val):
    global FACTORIAL_CACHE, FACTORIAL_CACHE_MAX_SIZE, FACTORIAL_CACHE_QUEUE

    if key in FACTORIAL_CACHE:
        FACTORIAL_CACHE_QUEUE.remove(key)
        FACTORIAL_CACHE_QUEUE.append(key)
        return

    if len(FACTORIAL_CACHE) >= FACTORIAL_CACHE_MAX_SIZE:
        popped_key = FACTORIAL_CACHE_QUEUE.popleft()
        FACTORIAL_CACHE.pop(popped_key)

    FACTORIAL_CACHE[key] = val
    FACTORIAL_CACHE_QUEUE.append(key)


def server():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=4))
    numstore_pb2_grpc.add_NumStoreServicer_to_server(
        NumStoreServicer(), server)
    server.add_insecure_port('[::]:5440')
    print("Server listening on port:5440")
    server.start()
    server.wait_for_termination()
    print("Server terminated")


if __name__ == '__main__':
    server()
