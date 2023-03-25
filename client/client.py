import sys
import grpc
import random
import numpy as np
import kvstore_pb2
import kvstore_pb2_grpc
from time import time
from threading import Lock, Thread

# Globals
THREAD_COUNT = 8
REQUEST_COUNT = 100
PORT = 0
LOCK = Lock()
CACHE_HITS = []
REQ_TIMES = []
FACT_COUNT = 0


def random_requests():
    global REQ_TIMES
    global CACHE_HITS
    global FACT_COUNT

    # ADDR = f'127.0.0.1:{PORT}'
    # channel = grpc.insecure_channel(ADDR)
    channel = grpc.insecure_channel('server:5440')
    stub = kvstore_pb2_grpc.KVStoreStub(channel)

    for i in range(REQUEST_COUNT):
        # Choosing key/value pair
        thread_key = str(random.randint(1, 100))
        thread_value = str(random.randint(1, 15))

        # Choosing which request to send
        run_SetNum = random.choice([True, False])
        if run_SetNum:
            # running Put request
            t1 = time()
            resp = stub.Put(kvstore_pb2.PutRequest(
                key=thread_key, value=thread_value))
            t2 = time()
            with LOCK:
                REQ_TIMES.append(t2-t1)

        else:
            # running Get request
            t1 = time()
            request = kvstore_pb2.GetRequest(key=thread_key)
            resp = stub.Get(request)
            t2 = time()
            with LOCK:
                FACT_COUNT += 1
                CACHE_HITS.append(resp.hit)
                REQ_TIMES.append(t2-t1)


if __name__ == '__main__':
    if len(sys.argv) != 2:
        print('InvalidCommandUsage\tUsage: python3 client.py <PORT_NUMBER>')
        sys.exit(1)

    PORT = int(sys.argv[1])
    # print(len(sys.argv))
    counter = 0
    running_threads = []

    # Start all threads
    while (counter < THREAD_COUNT):
        t = Thread(target=random_requests)
        t.start()
        running_threads.append(t)
        counter += 1

    # Wait for threads to finish running
    for t in running_threads:
        t.join()

    # Printing Summary Stats
    print(f'Summary Statistics')
    # print(f'cache hit rate: {round(np.count_nonzero(np.array(CACHE_HITS))/FACT_COUNT*100, 2)}%\np50 response time: {np.percentile(REQ_TIMES, q=50)} seconds\np99 response time: {np.percentile(REQ_TIMES, q=99)} seconds')
    print(
        f'\nRequest split for this run\nNumSet: {round((800-FACT_COUNT)/800*100,2)}% Fact: {round((FACT_COUNT)/800*100,2)}%')
