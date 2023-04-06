from concurrent.futures import ThreadPoolExecutor, as_completed
import client
import random
import time
import numpy as np


def perf_get_rpc_latency():
    latencies = []
    batch_throughputs = []
    for thread_count in range(1, 500, 10):
        batch = []
        print(f"Testing with {thread_count} threads")
        with ThreadPoolExecutor(max_workers=thread_count) as executor:
            key = f"KEY-{random.randint(1, pow(10, 10))}"
            t1 = time.time()
            future_calls = {executor.submit(client.send_get, key) for _ in range(thread_count)}
            for completed_task in as_completed(future_calls):
                batch.append(completed_task.result()[0])
            t2 = time.time()
            batch_throughputs.append(f"{thread_count / (t2 - t1)} req/s")
        latencies.append((thread_count, batch))
    # Thread batch-wise throughput
    print(batch_throughputs)
    # Thread batch-wise min, average, max, 99p latency
    lat_res = [(i[0], (
        min(i[1]),
        sum(i[1]) / len(i[1]),
        max(i[1]),
        np.percentile(i[1], 99)
    )) for i in latencies]

    for e in lat_res:
        print(e[0], e[1])

    print("Done")


def perf_put_rpc_latency():
    latencies = []
    batch_throughputs = []
    for thread_count in range(1, 300, 20):
        batch = []
        print(f"Testing with {thread_count} threads")
        with ThreadPoolExecutor(max_workers=thread_count) as executor:
            key = f"KEY-{random.randint(1, pow(10, 10))}"
            t1 = time.time()
            future_calls = {executor.submit(client.send_put, key, key) for _ in range(thread_count)}
            for completed_task in as_completed(future_calls):
                batch.append(completed_task.result())
            t2 = time.time()
            batch_throughputs.append(f"{thread_count / (t2 - t1)} req/s")
        latencies.append((thread_count, batch))
    # Thread batch-wise throughput
    print(batch_throughputs)
    # Thread batch-wise min, average, max, 99p latency
    lat_res = [(i[0], (
        min(i[1]),
        sum(i[1]) / len(i[1]),
        max(i[1]),
        np.percentile(i[1], 99)
    )) for i in latencies]

    for e in lat_res:
        print(e[0], e[1])

    print("Done")


def perf_degradation():
    logs_added_inbetween = 100

    put_times, get_times = [], []

    for iterations in range(10):
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
    print(f"GET: {get_times}")


if __name__ == "__main__":
    perf_get_rpc_latency()
    # perf_put_rpc_latency()
    # perf_degradation()
