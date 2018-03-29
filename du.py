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


def get_list(bucket):
    items = PROXY.object_list(ACCOUNT, bucket)
    todo = []
    _size = 0
    _files = 0
    for entry in items['objects']:
        if entry['name'].endswith('/'):
            todo.append(container_hierarchy(bucket, entry['name']))
        else:
            _size += entry['size']
            _files += 1

    return _files, _size, todo


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
            # print(element)
            yield element

        while listing:
            kwargs['marker'] = listing[-1][0]
            listing = PROXY.container_list(
                ACCOUNT, **kwargs)
            if listing:
                for element in listing:
                    # print(element)
                    yield element

    # listing = full_list()


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

    items = [container_hierarchy(bucket, path)]
    files = 0
    size = 0

    """
    # slow method
    while items:
        new_files, new_size, new_items = get_list(items.pop())
        items += new_items
        files += new_files
        size += new_size

    print("found %d files, %s bytes" % (files, size))
    """

    SUM = {}

    # fast method
    files = 0
    size = 0
    _bucket = container_hierarchy(bucket, path)
    # containers = PROXY.container_list(ACCOUNT, prefix=container_hierarchy(bucket, path), limit=9999)
    for entry in full_list(prefix=container_hierarchy(bucket, path)):
        name, _files, _size, _ = entry
        if name != _bucket and not name.startswith(_bucket + '%2F'):
            continue
        size += _size
        files += _files

        items = name.split('%2F')
        while items:
            _name = '/'.join(items)
            if not _name.startswith(args.path):
                break
            if _name in SUM:
                SUM[_name] += _size
            else:
                SUM[_name] = _size
            items.pop()

    view = [ (v, k) for k, v in SUM.items() ]
    view.sort()
    for v, k in view:
        print("%8d  %s" % (v, k))

    print("found %d files, %s bytes" % (files, size))


if __name__ == "__main__":
    main()
