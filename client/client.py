import sys
import grpc
import random
from time import sleep

sys.path.append('../')

from time import time
import kvstore_pb2
import kvstore_pb2_grpc
from threading import Lock, Thread

# Globals
THREAD_COUNT = 1
REQUEST_COUNT = 10
LOCK = Lock()
REQ_TIMES = []


def random_requests():
    global REQ_TIMES

    # ADDR = f'127.0.0.1:{PORT}'
    channel = grpc.insecure_channel('localhost:5440')
    stub = kvstore_pb2_grpc.KVStoreStub(channel)

    for i in range(REQUEST_COUNT):
        # Choosing key/value pair
        thread_key = str(random.randint(1, 3))
        thread_value = str(random.randint(1, 40))

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
            print(f'[LOG] for {thread_key}, got {resp.value} {resp.key_exists}')
            t2 = time()
            with LOCK:
                REQ_TIMES.append(t2 - t1)


def send_put(key, val):
    channel = grpc.insecure_channel('localhost:5440')
    stub = kvstore_pb2_grpc.KVStoreStub(channel)

    resp = stub.Put(kvstore_pb2.PutRequest(key=key, value=val))
    print(f"PUT {key}:{val} sent! Response error: {resp.error}")


def send_get(key):
    channel = grpc.insecure_channel('localhost:5440')
    stub = kvstore_pb2_grpc.KVStoreStub(channel)

    resp = stub.Get(kvstore_pb2.GetRequest(key=key))
    print(f"GET {key} sent! Response: {resp.key_exists}, {resp.key}, {resp.value}")


if __name__ == '__main__':
    counter = 0
    running_threads = []

    # Spawn multiple threads and sent random get/put requests
    # sleep(2)
    #
    # # Start all threads
    # while counter < THREAD_COUNT:
    #     t = Thread(target=random_requests)
    #     t.start()
    #     running_threads.append(t)
    #     counter += 1
    #
    # # Wait for threads to finish running
    # for t in running_threads:
    #     t.join()

    # Send single put and 2 gets (one valid one invalid)
    send_put("Key1", "Val1")
    send_get("Key1")
    send_get("Invalid")

    print(f'Completed Client Process!')
