#!/usr/bin/env python
"""
"""

from __future__ import print_function
import argparse
import os
# from Queue import Queue
from eventlet import Queue
import eventlet
from oio.api.object_storage import ObjectStorageApi


NS = None
ACCOUNT = None
PROXY = None
VERBOSE = False


def worker_objects():
    proxy = ObjectStorageApi(NS)
    while True:
        try:
            name = QUEUE.get(timeout=5)
        except Queue.empty:
            break

        try:
            items = proxy.object_list(ACCOUNT, name)
            objs = [_item['name'] for _item in items['objects']]
            if VERBOSE:
                print("Deleting", len(objs), "objects")
            proxy.object_delete_many(ACCOUNT, name, objs=objs)
        except Exception as ex:
            print("Objs %s: %s" % (name, str(ex)))

        QUEUE.task_done()


def worker_container():
    proxy = ObjectStorageApi(NS)
    while True:
        try:
            name = QUEUE.get(timeout=5)
        except Queue.empty:
            break

        if VERBOSE:
            print("Deleting", name)
        try:
            proxy.container_delete(ACCOUNT, name)
        except Exception as ex:
            print("Container %s: %s" % (name, str(ex)))

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
    parser.add_argument("--max-worker", default=1, type=int)
    parser.add_argument("--verbose", default=False, action="store_true")
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

    global ACCOUNT, PROXY, QUEUE, NS, VERBOSE
    ACCOUNT = args.account
    NS = args.namespace
    VERBOSE = args.verbose
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
        QUEUE.join()

        QUEUE = Queue()
        for i in range(num_worker_threads):
            pool.spawn(worker_container)

        print("We have to delete", len(containers), "containers")

        for container in containers:
            QUEUE.put(container)

        QUEUE.join()


if __name__ == "__main__":
    main()
