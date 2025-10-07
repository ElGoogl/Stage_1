import time
import statistics
from memory_profiler import memory_usage

# Generic benchmarking helper
def benchmark_function(func, *args, runs=1, **kwargs):
    """Run a function multiple times and return average time + memory."""
    times, memories = [], []
    for _ in range(runs):
        start = time.perf_counter()
        mem_usage = memory_usage((func, args, kwargs))
        end = time.perf_counter()
        times.append(end - start)
        memories.append(max(mem_usage))
    return statistics.mean(times), statistics.mean(memories)