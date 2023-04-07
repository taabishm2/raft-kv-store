from concurrent.futures import ThreadPoolExecutor, as_completed
import client
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
    for thread_count in range(1, 1000, 1):
        batch = []
        print(f"Testing with {thread_count} threads")
        with ThreadPoolExecutor(max_workers=thread_count) as executor:
            key = f"KEY-{random.randint(1, pow(10, 10))}"
            t1 = time.time()
            future_calls = {executor.submit(client.send_get, key) for _ in range(thread_count)}
            for completed_task in as_completed(future_calls):
                batch.append(completed_task.result()[0])
            t2 = time.time()
            batch_throughputs.append((thread_count, thread_count / (t2 - t1)))
        latencies.append((thread_count, batch))
    # Thread batch-wise throughput
    x, y = zip(*batch_throughputs)
    plt.plot(x, y)
    plt.title(f'graphs/{NUM_SERVERS}-server-GET-parallel-throughputs.png')
    plt.savefig(f'graphs/{NUM_SERVERS}-server-GET-parallel-throughputs.png')
    plt.clf()

    # Thread batch-wise min, average, max, 99p latency
    lat_res = [(i[0],
        np.percentile(i[1], 1),
        sum(i[1]) / len(i[1]),
        np.percentile(i[1], 99)
    ) for i in latencies]

    plt.plot(x, [y[1] for y in lat_res])
    plt.plot(x, [y[2] for y in lat_res])
    plt.plot(x, [y[3] for y in lat_res])

    plt.title(f'graphs/{NUM_SERVERS}-server-GET-parallel-latencies.png')
    plt.savefig(f'graphs/{NUM_SERVERS}-server-GET-parallel-latencies.png')
    plt.clf()

    print("Done")


# TODO: This must clear log entries after each iteration
def perf_put_rpc_latency():
    latencies = []
    batch_throughputs = []
    for thread_count in range(1, 100, 2):
        batch = []
        print(f"Testing with {thread_count} threads")
        with ThreadPoolExecutor(max_workers=thread_count) as executor:
            key = f"KEY-{random.randint(1, pow(10, 10))}"
            t1 = time.time()
            future_calls = {executor.submit(client.send_put, key, key) for _ in range(thread_count)}
            for completed_task in as_completed(future_calls):
                batch.append(completed_task.result())
            t2 = time.time()
            batch_throughputs.append((thread_count, thread_count / (t2 - t1)))
        latencies.append((thread_count, batch))
    # Thread batch-wise throughput
    x, y = zip(*batch_throughputs)
    plt.plot(x, y)
    plt.title(f'graphs/{NUM_SERVERS}-server-PUT-parallel-throughputs.png')
    plt.savefig(f'graphs/{NUM_SERVERS}-server-PUT-parallel-throughputs.png')
    plt.clf()

    # Thread batch-wise min, average, max, 99p latency
    lat_res = [(i[0],
        np.percentile(i[1], 1),
        sum(i[1]) / len(i[1]),
        np.percentile(i[1], 99)
    ) for i in latencies]

    plt.plot(x, [y[1] for y in lat_res])
    plt.plot(x, [y[2] for y in lat_res])
    plt.plot(x, [y[3] for y in lat_res])

    plt.title(f'graphs/{NUM_SERVERS}-server-PUT-parallel-latencies.png')
    plt.savefig(f'graphs/{NUM_SERVERS}-server-PUT-parallel-latencies.png')
    plt.clf()

    print("Done")


def perf_degradation():
    logs_added_inbetween = 250

    put_times, get_times = [], []

    for iterations in range(50):
        print("Iteration ", iterations)
        with ThreadPoolExecutor(max_workers=logs_added_inbetween) as executor:
            key = f"KEY-{random.randint(1, pow(10, 10))}"
            future_calls = {executor.submit(client.send_put, key, key) for _ in range(logs_added_inbetween)}
            for completed_task in as_completed(future_calls):
                pass

        tw, tr = 0, 0
        avg_over = 3
        for _ in range(avg_over):
            tw += client.send_put("Key", "Val")
            tr += client.send_get("Key")[0]
        put_times.append(tw/avg_over)
        get_times.append(tr/avg_over)

    print(f"PUT: {put_times}")
    plt.plot(put_times)
    plt.title(f'graphs/{NUM_SERVERS}-server-PUT-latency-degrade.png')
    plt.savefig(f'graphs/{NUM_SERVERS}-server-PUT-latency-degrade.png')
    plt.clf()

    print(f"GET: {get_times}")
    plt.plot(get_times)
    plt.title(f'graphs/{NUM_SERVERS}-server-GET-latency-degrade.png')
    plt.savefig(f'graphs/{NUM_SERVERS}-server-GET-latency-degrade.png')
    plt.clf()

    print(f"GET: {get_times}")


def single_thread_throughput():
    ctr_dict = defaultdict(int)
    td = time.time()
    for i in range(200):
        key = f"key-{random.randint(1, 1000000000)}"
        try:
            resp = client.send_put_for_val(key, key)
            if resp.error or resp.is_redirect:
                #print("ERR")
                ctr_dict[time.time() - td] += 0
            else:
                #print("OK")
                ctr_dict[time.time() - td] += 1
        except:
            #print("Ex")
            ctr_dict[time.time() - td] += 0
        #break

    sorted_keys = sorted(ctr_dict.keys())
    y = [ctr_dict[key] for key in sorted_keys]

    plt.plot(sorted_keys, y)
    plt.title(f'graphs/{NUM_SERVERS}-server-PUT-singlethread-throughput.png')
    plt.savefig(f'graphs/{NUM_SERVERS}-server-PUT-singlethread-throughput.png')
    plt.clf()


# def smoothen(data, window_size=3):
#     max_val, min_val = window_size
#     for i in range(1, len(data) - 1):
#         if data[i]


if __name__ == "__main__":
    #perf_get_rpc_latency()
    # perf_put_rpc_latency()
    perf_degradation()
    # #single_thread_throughput()