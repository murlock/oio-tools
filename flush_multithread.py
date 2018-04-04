#!/usr/bin/env python
"""
"""

from __future__ import print_function
import argparse
import os
import threading
import eventlet
from eventlet import Queue
import sys
import time
from oio.api.object_storage import ObjectStorageApi

eventlet.monkey_patch()


NS = None
ACCOUNT = None
PROXY = None
VERBOSE = False
TIMEOUT = 5


class AtomicInteger():
    def __init__(self):
        self._files = 0
        self._size = 0
        self._lock = threading.Lock()

    def add(self, files, size):
        with self._lock:
            self._files += files
            self._size += size

    def reset(self):
        with self._lock:
            val = (self._files, self._size)
            self._files = 0
            self._size = 0
            return val


COUNTERS = AtomicInteger()


def worker_objects():
    proxy = ObjectStorageApi(NS)
    while True:
        try:
            name = QUEUE.get(timeout=TIMEOUT)
        except eventlet.queue.Empty:
            if VERBOSE:
                print("Leaving worker")
            break

        while True:
            try:
                items = proxy.object_list(ACCOUNT, name)
                objs = [_item['name'] for _item in items['objects']]
                size = sum([_item['size'] for _item in items['objects']])
                if len(objs) == 0:
                    break
                if VERBOSE:
                    print("Deleting", len(objs), "objects")
                proxy.object_delete_many(ACCOUNT, name, objs=objs)
                COUNTERS.add(len(objs), size)
                break
            except Exception as ex:
                if "Election failed" in str(ex):
                    # wait default Election wait delay
                    time.sleep(20)
                    continue
                print("Objs %s: %s" % (name, str(ex)), file=sys.stderr)
                break

        QUEUE.task_done()


def worker_container():
    proxy = ObjectStorageApi(NS)
    while True:
        try:
            name = QUEUE.get(timeout=TIMEOUT)
        except eventlet.queue.Empty:
            break

        while True:
            if VERBOSE:
                print("Deleting", name)
            try:
                proxy.container_delete(ACCOUNT, name)
                COUNTERS.add(1, 0)
                break
            except Exception as ex:
                if "Election failed" in str(ex):
                    # wait default Election wait delay
                    time.sleep(20)
                    continue
                print("Container %s: %s" % (name, str(ex)), file=sys.stderr)
                break

        QUEUE.task_done()


def container_hierarchy(bucket, path):
    if not path:
        return bucket
    ch = '%2F'.join(path.rstrip('/').split('/'))
    return bucket + '%2F' + ch


def options():
    parser = argparse.ArgumentParser()
    parser.add_argument("--account", default=os.getenv("OIO_ACCOUNT", "demo"))
    parser.add_argument("--namespace", default=os.getenv("OIO_NS", "OPENIO"))
    parser.add_argument("--max-worker", default=20, type=int)
    parser.add_argument("--verbose", default=False, action="store_true")
    parser.add_argument("--timeout", default=5, type=int)
    parser.add_argument("--report", default=60, type=int,
                        help="Report progress every X seconds")
    parser.add_argument("path", nargs='+', help="bucket/path1/path2")

    return parser.parse_args()


def full_list(**kwargs):
        listing = PROXY.container_list(
            ACCOUNT, **kwargs)
        for element in listing:
            yield element

        while listing:
            kwargs['marker'] = listing[-1][0]
            listing = PROXY.container_list(
                ACCOUNT, **kwargs)
            if listing:
                for element in listing:
                    yield element


def main():
    args = options()

    global ACCOUNT, PROXY, QUEUE, NS, VERBOSE, TIMEOUT
    ACCOUNT = args.account
    NS = args.namespace
    VERBOSE = args.verbose
    TIMEOUT = args.timeout
    PROXY = ObjectStorageApi(NS)

    num_worker_threads = int(args.max_worker)
    print("Using %d workers" % num_worker_threads)

    for path in args.path:
        path = path.rstrip('/')
        if '/' in path:
            bucket, path = path.split('/', 1)
        else:
            bucket = path
            path = ""

        containers = []

        QUEUE = Queue()
        pool = eventlet.GreenPool(num_worker_threads)

        for i in range(num_worker_threads):
            pool.spawn(worker_objects)

        _bucket = container_hierarchy(bucket, path)
        # we don't use placeholders, we use prefix path as prefix
        for entry in full_list(prefix=container_hierarchy(bucket, path)):
            name, _files, _size, _ = entry
            if name != _bucket and not name.startswith(_bucket + '%2F'):
                continue

            if _files:
                QUEUE.put(name)

            containers.append(name)

        # we have to wait all objects
        print("Waiting flush of objects")

        report = args.report

        while not QUEUE.empty():
            ts = time.time()
            while time.time() - ts < report and not QUEUE.empty():
                time.sleep(1)
            diff = time.time() - ts
            val = COUNTERS.reset()
            print("Objects: %5.2f / Size: %5.2f" % (
                    val[0] / diff, val[1] / diff), end='\r')
            sys.stdout.flush()

        print("Waiting end of workers")
        QUEUE.join()

        QUEUE = Queue()
        for i in range(num_worker_threads):
            pool.spawn(worker_container)

        print("We have to delete", len(containers), "containers")

        for container in containers:
            QUEUE.put(container)

        while not QUEUE.empty():
            ts = time.time()
            while time.time() - ts < report and not QUEUE.empty():
                time.sleep(1)
            diff = time.time() - ts
            val = COUNTERS.reset()
            print("Containers: %5.2f" % (val[0] / diff), end='\r')
            sys.stdout.flush()

        QUEUE.join()


if __name__ == "__main__":
    main()
