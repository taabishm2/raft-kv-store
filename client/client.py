import sys
import grpc
import random
from time import sleep

sys.path.append('../')

from time import time, sleep
import kvstore_pb2
import kvstore_pb2_grpc
import raft_pb2
import raft_pb2_grpc
from threading import Lock, Thread

# Globals
THREAD_COUNT = 1
REQUEST_COUNT = 10
LOCK = Lock()
REQ_TIMES = []
NODE_IPS = {
    "server-1": 'localhost:5440',
    "server-2": 'localhost:5441',
    "server-3": 'localhost:5442'}
LEADER_NAME = "server-2"


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
    global NODE_IPS, LEADER_NAME

    LEADER_IP = NODE_IPS[LEADER_NAME]
    channel = grpc.insecure_channel(LEADER_IP)
    stub = kvstore_pb2_grpc.KVStoreStub(channel)

    resp = stub.Put(kvstore_pb2.PutRequest(key=key, value=val))
    print(f"PUT {key}:{val} sent! Response error:{resp.error}, redirect:{resp.is_redirect}, \
        {resp.redirect_server}")

    if resp.is_redirect:
        LEADER_NAME = resp.redirect_server
        return send_put(key, val)


def send_get(key):
    global NODE_IPS, LEADER_NAME

    LEADER_IP = NODE_IPS[LEADER_NAME]
    channel = grpc.insecure_channel(LEADER_IP)
    stub = kvstore_pb2_grpc.KVStoreStub(channel)

    resp = stub.Get(kvstore_pb2.GetRequest(key=key))
    print(f"GET {key} sent! Response:{resp.key_exists}, key:{resp.key}, val:{resp.value},\
         redirect:{resp.is_redirect}, leader:{resp.redirect_server}")

    if resp.is_redirect:
        LEADER_NAME = resp.redirect_server
        return send_get(key)


def send_request_vote(term, candidate_id, logidx, logterm):
    channel = grpc.insecure_channel('localhost:4000')
    stub = raft_pb2_grpc.RaftProtocolStub(channel)

    resp = stub.RequestVote(raft_pb2.VoteRequest(term=term, candidate_id=candidate_id, last_log_index=logidx, last_log_term=logterm))
    print(f"Vote request sent! Response: {resp.term}, {resp.vote_granted}, {resp.error}")
    return resp


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
    # send_put("Key1", "Val1")
    #send_put("Key43", "Val534")
    send_get("Key43")

    #send_put("Key6", "Val6")
    #send_get("Key6")

    

    # Send a vote request
    # resp = send_request_vote(5, "sender-a", 5, 5)
    # assert resp.term == 5 and resp.vote_granted == True and resp.error == ""
    # resp = send_request_vote(5, "sender-b", 5, 5)
    # assert resp.term == 5 and resp.vote_granted == False and resp.error == ""

    print(f'Completed Client Process!')
