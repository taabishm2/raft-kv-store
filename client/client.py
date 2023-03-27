import sys
import grpc
import random

sys.path.append('../')

from time import time
import kvstore_pb2
import kvstore_pb2_grpc
from threading import Lock, Thread

# Globals
THREAD_COUNT = 8
REQUEST_COUNT = 100
LOCK = Lock()
REQ_TIMES = []


def random_requests():
    global REQ_TIMES

    channel = grpc.insecure_channel('localhost:5440')
    stub = kvstore_pb2_grpc.KVStoreStub(channel)

    for i in range(REQUEST_COUNT):
        # Choosing key/value pair
        thread_key = str(random.randint(1, 100))
        thread_value = str(random.randint(1, 15))

        # Choosing which request to send
        run_set_num = random.choice([True, False])
        if run_set_num:
            t1 = time()
            resp = stub.Put(kvstore_pb2.PutRequest(key=thread_key, value=thread_value))
            t2 = time()
            with LOCK:
                REQ_TIMES.append(t2 - t1)

        else:
            t1 = time()
            resp = stub.Get(kvstore_pb2.GetRequest(key=thread_key))
            print(resp.value)
            t2 = time()
            with LOCK:
                REQ_TIMES.append(t2 - t1)


if __name__ == '__main__':
    counter = 0
    running_threads = []

    # Start all threads
    while counter < THREAD_COUNT:
        t = Thread(target=random_requests)
        t.start()
        running_threads.append(t)
        counter += 1

    # Wait for threads to finish running
    for t in running_threads:
        t.join()

    print(f'Completed Client Process!')
