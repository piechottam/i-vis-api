from dask.multiprocessing import get

dask.config.set(scheduler="processes")
from concurrent.futures import ThreadPoolExecutor
import dask

dask.config.set(pool=ThreadPoolExecutor(4))

import time


def add(a, b):
    print(f"add: {str(a)} {str(b)}")
    time.sleep(2)
    return a + b


g = {
    "a": 1,
    "b": 2,
    "add1": (add, "a", "b"),
    "add2": (add, "a", "b"),
    "addS": (add, "add1", "add2"),
}
