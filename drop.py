#!/usr/bin/env python
"""
"""

from __future__ import print_function
import argparse
import os
from oio.api.object_storage import ObjectStorageApi


ACCOUNT = None
PROXY = None

def container_hierarchy(bucket, path):
    if not path:
        return bucket
    ch = '%2F'.join(path.rstrip('/').split('/'))
    return bucket + '%2F' + ch


def options():
    parser = argparse.ArgumentParser()
    parser.add_argument("--account", default=os.getenv("OIO_ACCOUNT", "demo"))
    parser.add_argument("--namespace", default=os.getenv("OIO_NS", "OPENIO"))
    parser.add_argument("path", help="bucket/path1/path2")

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

    global ACCOUNT, PROXY
    ACCOUNT = args.account
    PROXY = ObjectStorageApi("OPENIO")

    args.path = args.path.rstrip('/')
    if '/' in args.path:
        bucket, path = args.path.split('/', 1)
    else:
        bucket = args.path
        path = ""

    containers = []

    _bucket = container_hierarchy(bucket, path)
    # we don't use placeholders, we use prefix path as prefix
    for entry in full_list(prefix=container_hierarchy(bucket, path)):
        name, _files, _size, _ = entry
        if name != _bucket and not name.startswith(_bucket + '%2F'):
            continue

        if _files:
            items = PROXY.object_list(ACCOUNT, name)
            objs = [_item['name'] for _item in items['objects']]
            PROXY.object_delete_many(ACCOUNT, name, objs=objs)
            print("Deleting", len(objs), "objects")

        containers.append(name)

    print("We have to delete", len(containers), "containers")

    for container in containers:
        print("Deleting", container)
        PROXY.container_delete(ACCOUNT, container)


if __name__ == "__main__":
    main()
