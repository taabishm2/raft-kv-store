import shelve
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed
import client1
import random
import time
import numpy as np
from matplotlib import pyplot as plt
import numpy as np
from collections import defaultdict

NUM_SERVERS = 3

def perf_get_rpc_latency():
    latencies = []
    batch_throughputs = []
    # Do 1, 1000, 1
    for thread_count in range(1, 1000, 100):
        batch = []
        print(f"Testing with {thread_count} threads")
        with ThreadPoolExecutor(max_workers=thread_count) as executor:
            key = f"KEY-{random.randint(1, pow(10, 10))}"
            t1 = time.time()
            future_calls = {executor.submit(client1.send_get, key) for _ in range(thread_count)}
            for completed_task in as_completed(future_calls):
                batch.append(completed_task.result()[0])
            t2 = time.time()
            batch_throughputs.append((thread_count, thread_count / (t2 - t1)))
        latencies.append((thread_count, batch))
    # Thread batch-wise throughput
    x, y = zip(*batch_throughputs)
    plt.plot(x, y)
    plt.xlabel("Count of parallel requests")
    plt.ylabel("Observed throughput (req/sec)")
    plt.title(f'Parallel GET requests vs throughput for {NUM_SERVERS} servers')
    plt.savefig(f'graphs/{NUM_SERVERS}-server-GET-parallel-throughputs.png')
    plt.clf()

    # Thread batch-wise min, average, max, 99p latency
    lat_res = [(i[0],
        np.percentile(i[1], 1),
        sum(i[1]) / len(i[1]),
        np.percentile(i[1], 99)
    ) for i in latencies]

    plt.plot(x, [y[1] for y in lat_res], label="1 percentile")
    plt.plot(x, [y[2] for y in lat_res], label="Average")
    plt.plot(x, [y[3] for y in lat_res], label="99 percentile")
    plt.xlabel("Count of parallel requests")
    plt.ylabel("Observed latency (sec)")
    plt.legend()
    plt.title(f'Parallel GET requests vs latency for {NUM_SERVERS} servers')
    plt.savefig(f'graphs/{NUM_SERVERS}-server-GET-parallel-latencies.png')
    plt.clf()

    print("Done")


# TODO: This must clear log entries after each iteration
def perf_put_rpc_latency():
    latencies = []
    batch_throughputs = []
    for thread_count in range(1, 500, 20):
        batch = []
        print(f"Testing with {thread_count} threads")
        with ThreadPoolExecutor(max_workers=thread_count) as executor:
            key = f"KEY-{random.randint(1, pow(10, 10))}"
            t1 = time.time()
            future_calls = {executor.submit(client1.send_put, key, key) for _ in range(thread_count)}
            for completed_task in as_completed(future_calls):
                batch.append(completed_task.result())
            t2 = time.time()
            batch_throughputs.append((thread_count, thread_count / (t2 - t1)))
        latencies.append((thread_count, batch))
    # Thread batch-wise throughput
    x, y = zip(*batch_throughputs)
    plt.plot(x, y)
    plt.xlabel("Count of parallel requests")
    plt.ylabel("Observed throughput (req/sec)")
    plt.title(f'Parallel PUT requests vs throughput for {NUM_SERVERS} servers')
    plt.savefig(f'graphs/{NUM_SERVERS}-server-PUT-parallel-throughputs.png')
    plt.clf()

    # Thread batch-wise min, average, max, 99p latency
    lat_res = [(i[0],
        np.percentile(i[1], 1),
        sum(i[1]) / len(i[1]),
        np.percentile(i[1], 99)
    ) for i in latencies]

    plt.plot(x, [y[1] for y in lat_res], label="1 percentile")
    plt.plot(x, [y[2] for y in lat_res], label="Average")
    plt.plot(x, [y[3] for y in lat_res], label="99 percentile")
    plt.xlabel("Count of parallel requests")
    plt.ylabel("Observed latency (sec)")
    plt.title(f'Parallel PUT requests vs latency for {NUM_SERVERS} servers')
    plt.savefig(f'graphs/{NUM_SERVERS}-server-PUT-parallel-latencies.png')
    plt.clf()

    print("Done")


def perf_degradation():
    # User 250
    logs_added_inbetween = 10
    # Use 50
    iterations = 10

    put_times, get_times = [], []

    for iterations in range(iterations):
        print("Iteration ", iterations, " of ", iterations)
        with ThreadPoolExecutor(max_workers=logs_added_inbetween) as executor:
            key = f"KEY-{random.randint(1, pow(10, 10))}"
            future_calls = {executor.submit(client1.send_put, key, key) for _ in range(logs_added_inbetween)}
            for completed_task in as_completed(future_calls):
                pass

        tw, tr = 0, 0
        avg_over = 3
        for _ in range(avg_over):
            tw += client1.send_put("Key", "Val")
            tr += client1.send_get("Key")[0]
        put_times.append(tw/avg_over)
        get_times.append(tr/avg_over)

    plt.plot([i for i in range(0, iterations * logs_added_inbetween + 1, logs_added_inbetween)], put_times)
    plt.xlabel("Count of log entries")
    plt.ylabel("Observed latency (sec)")
    plt.title(f'Log size vs PUT latency for {NUM_SERVERS} servers')
    plt.savefig(f'graphs/{NUM_SERVERS}-server-PUT-latency-degrade.png')
    plt.clf()

    plt.plot([i for i in range(0, iterations * logs_added_inbetween + 1, logs_added_inbetween)], get_times)
    plt.xlabel("Count of log entries")
    plt.ylabel("Observed latency (sec)")
    plt.legend()
    plt.title(f'Log size vs GET latency for {NUM_SERVERS} servers')
    plt.savefig(f'graphs/{NUM_SERVERS}-server-GET-latency-degrade.png')
    plt.clf()

def single_thread_throughput():
    ctr_dict = defaultdict(int)
    td = time.time()
    for i in range(200):
        key = f"key-123"
        try:
            resp = client1.send_put_for_val(key, key)
            if resp.error or resp.is_redirect:
                #print("ERR")
                ctr_dict[int(time.time() - td)] += 0
            else:
                #print("OK")
                ctr_dict[int(time.time() - td)] += 1
        except:
            #print("Ex")
            ctr_dict[int(time.time() - td)] += 0
        #break

    print(ctr_dict)
    sorted_keys = sorted(ctr_dict.keys())
    y = [ctr_dict[key] for key in sorted_keys]

    plt.plot(sorted_keys, y)
    plt.xlabel("Time (seconds)")
    plt.ylabel("Observed PUT throughput (req/sec)")
    plt.title(f'Single threaded PUT throughput for {NUM_SERVERS} servers')
    plt.savefig(f'graphs/{NUM_SERVERS}-server-PUT-singlethread-throughput.png')
    plt.clf()

def single_thread_throughput_leader_killed():
    ctr_dict = defaultdict(int)
    td = time.time()
    print("KILL LEADER !!!!")
    for i in range(200):
        key = f"key-123"
        try:
            resp = client1.send_put_for_val(key, key)
            if resp.error or resp.is_redirect:
                #print("ERR")
                ctr_dict[int(time.time() - td)] += 0
            else:
                #print("OK")
                ctr_dict[int(time.time() - td)] += 1
        except:
            #print("Ex")
            ctr_dict[int(time.time() - td)] += 0
        #break

    #print(ctr_dict)
    sorted_keys = sorted(ctr_dict.keys())
    y = [ctr_dict[key] for key in sorted_keys]

    plt.plot(sorted_keys, y)
    plt.xlabel("Time (seconds)")
    plt.ylabel("Observed PUT throughput (req/sec)")
    plt.title(f'Availability on leader crash for {NUM_SERVERS} servers')
    plt.savefig(f'graphs/{NUM_SERVERS}-server-PUT-availability-singlethread-throughput.png')
    plt.clf()

def log_recovery_time():
    xaxis, yaxis = [], []
    for log_diff in range(10, 1000, 10):

        # Since leader might change, re-fetch
        leader_ip = client1.LEADER_NAME
        g = client1.send_get("K")[1]
        if g.is_redirect:
            leader_ip = g.redirect_server
        ips_logs = {"server-1": "server1", "server-2": "server2", "server-3": "server3"}
        ips_logs.pop(leader_ip)
        to_be_killed = ips_logs.keys()[0]
        print(f"Leader: {leader_ip}, to be killed: {to_be_killed}")

        xaxis.append(log_diff)
        # kill to_be_killed with remove node
        client1.kill_node(to_be_killed, ips_logs.keys())

        # Make 'log_diff' appends to leader
        with ThreadPoolExecutor(max_workers=log_diff - 1) as executor:
            future_calls = {executor.submit(client1.send_put, "KEY", "VAL") for _ in range(log_diff - 1)}
            for completed_task in as_completed(future_calls):
                pass
        # Add one final key which will be checked later
        last_key = "LASTKEY" + str(log_diff)
        client1.send_put(last_key, last_key)

        leader_shelve = shelve.open(f"../logs/logcache/server{leader_ip[-1]}/stable_log")
        leader_log_size = leader_shelve["SHELF_SIZE"]

        # Re-add dead node and wait till it sees last_key
        while True:
            t1 = time.time()
            # add killed node back
            add_thread = threading.Thread(target=client1.add_node, args=(to_be_killed, ips_logs.keys()))
            # directly open shelve log of killed node
            dead_node_shelve = shelve.open(f"../logs/logcache/server{to_be_killed[-1]}/stable_log")
            log_size = dead_node_shelve["SHELF_SIZE"]

            if log_size >= leader_log_size:
                yaxis.append(time.time() - t1)
                break

        plt.plot(xaxis, yaxis)
        plt.xlabel("Count of entries missing in recovered node")
        plt.ylabel("Time taken for log to become up-to-date")
        plt.title(f'Dead-node log recovery time for {NUM_SERVERS} servers')
        plt.savefig(f'graphs/{NUM_SERVERS}-server-dead-node-recoverytime.png')
        plt.clf()

# def smoothen(data, window_size=3):
#     max_val, min_val = window_size
#     for i in range(1, len(data) - 1):
#         if data[i]


if __name__ == "__main__":
    input("[REQUIRED] Run `chmod -R a+rw /local-path-of-logs-directory-here`")
    # perf_get_rpc_latency()
    # perf_put_rpc_latency()
    # perf_degradation()
    # single_thread_throughput()
    single_thread_throughput_leader_killed()
