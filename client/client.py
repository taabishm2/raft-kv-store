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
    "server-3": 'localhost:5442',
    "server-4": 'localhost:5443'}
LEADER_NAME = "server-1"


def random_requests():
    global REQ_TIMES

    # ADDR = f'127.0.0.1:{PORT}'
    channel = grpc.insecure_channel('localhost:5441')
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
    else:
        return resp


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
    else:
        return resp


def send_request_vote(term, candidate_id, logidx, logterm):
    channel = grpc.insecure_channel('localhost:4000')
    stub = raft_pb2_grpc.RaftProtocolStub(channel)

    resp = stub.RequestVote(raft_pb2.VoteRequest(term=term, candidate_id=candidate_id, last_log_index=logidx, last_log_term=logterm))
    print(f"Vote request sent! Response: {resp.term}, {resp.vote_granted}, {resp.error}")
    return resp

def send_add_node(peer_ip):
    for i in range(len(NODE_IPS)):
        print(f"==localhost:400{i}")
        channel = grpc.insecure_channel(f"localhost:400{i}")
        stub = raft_pb2_grpc.RaftProtocolStub(channel)
        resp = stub.AddNode(raft_pb2.NodeRequest(peer_ip=peer_ip))
        print(f"Add Node for {peer_ip} sent! Response error:{resp.error}")

def send_remove_node(peer_ip):
    for i, node in enumerate(NODE_IPS):
    # for i in range(len(NODE_IPS)):
        if peer_ip == NODE_IPS[node]:
            continue
        print(f"==localhost:400{i}")
        channel = grpc.insecure_channel(f"localhost:400{i}")
        stub = raft_pb2_grpc.RaftProtocolStub(channel)
        resp = stub.RemoveNode(raft_pb2.NodeRequest(peer_ip=peer_ip))
        print(f"Add Node for {peer_ip} sent! Response error:{resp.error}")


def basic_consistency_test():
    # Try to get a value not written
    resp = send_get("Key0")
    # Send single put and 2 gets (one valid one invalid)
    # send_put("Key1", "Val1")
    for i in range(10):
        send_put(f"Key{i}", f"Val{i}")
    send_put("Key43", "Val534")
    send_get("Key43")

    send_put("Key6", "Val6")
    send_get("Key6")

    # send_add_node("server-4:4000")

    send_put("Key3", "Val3")
    send_get("Key1")

    send_put("Key2", "Val2")
    send_get("Key2")


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
    send_put("Key43", "Val534")
    send_get("Key43")

    send_put("Key6", "Val6")
    send_get("Key6")

    # send_add_node("server-4:4000")

    send_put("Key3", "Val3")
    send_get("Key1")

    send_put("Key2", "Val2")
    send_get("Key2")

    send_remove_node("server-4:4000")

    print(f'Completed Client Process!')
