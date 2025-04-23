import threading
import time
import random
import string
from rediscluster import RedisCluster

# Redis Cluster connection details
startup_nodes = [
    {"host": "10.128.0.2", "port": "6379"},
    {"host": "10.128.0.3", "port": "6379"},
    {"host": "10.128.0.4", "port": "6379"},
    {"host": "10.128.0.5", "port": "6379"},
    {"host": "10.128.0.6", "port": "6379"},
    {"host": "10.128.0.7", "port": "6379"},
]

# Connect to Redis Cluster
rc = RedisCluster(startup_nodes=startup_nodes, decode_responses=True)

# Global counters
operation_count = 0
latency_total = 0
lock = threading.Lock()


def random_string(length=8):
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


def worker():
    global operation_count, latency_total
    while True:
        key = random_string(8)
        value = random_string(16)

        start_time = time.time()
        rc.set(key, value)
        rc.get(key)
        latency = (time.time() - start_time) * 1000  # milliseconds

        with lock:
            operation_count += 2  # one SET + one GET
            latency_total += latency


def monitor():
    global operation_count, latency_total
    while True:
        time.sleep(5)  # Report every 5 seconds
        with lock:
            ops = operation_count
            avg_latency = latency_total / operation_count if operation_count else 0
            operation_count = 0
            latency_total = 0
        print(f"[5s Report] Ops/sec: {ops / 5:.2f}, Avg Latency: {avg_latency:.2f} ms")


if __name__ == "__main__":
    # Start multiple worker threads
    for _ in range(10):
        threading.Thread(target=worker, daemon=True).start()

    # Start monitor thread
    monitor()
