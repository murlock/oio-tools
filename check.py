#!/usr/bin/env python

from __future__ import print_function
from time import time
import argparse
import sys
import os
import json

from oio.directory.client import DirectoryClient
from oio.common.http import get_pool_manager
from oio.account.backend import AccountBackend
from oio.common.exceptions import NotFound
from oio.conscience.client import LbClient

ACCOUNT = "murlock"
NAMESPACE = "OPENIO"
HOST = '127.0.0.1:6035'


def options(args):
    parser = argparse.ArgumentParser()
    parser.add_argument("--account", default=os.getenv("OIO_ACCOUNT", "demo"))
    parser.add_argument("--namespace", default=os.getenv("OIO_NS", "OPENIO"))
    parser.add_argument("--verbose", default=False, action="store_true")
    parser.add_argument("--dry-run", default=False, action="store_true")
    parser.add_argument("account", help="IP:PORT of Account service")

    return parser.parse_args()

def run(args):
    pool = get_pool_manager()

    dirclient = DirectoryClient({"namespace": NAMESPACE})
    backend = AccountBackend(dirclient.conf)
    lst = backend.list_containers(ACCOUNT)

    for entry, _, _, _ in lst:
        try:
            dirclient.show(account=ACCOUNT, reference=entry)
            if args.verbose:
                print("%s: OK" % entry)
            continue
        except NotFound:
            pass
        except Exception as exc:
            print("Exception not managed for %s: %s" % (entry, str(exc)))
            continue
        print("%s: meta2 not found", entry)
        if args.dry_run:
            continue

        data = {"dtime": time(), "name": entry}
        # post event to Account service
        res = pool.request('POST', HOST + '/v1.0/account/container/update?id=%s' % ACCOUNT,
            headers={'Content-Type': 'application/json'},
            body=json.dumps(data))
        if res.status_int / 100 != 2:
            print(res.status)


if __name__ == "__main__":
    args = options(sys.argv)
    ACCOUNT = args.account
    NAMESPACE =  args.namespace
    HOST = args.account
    run(args)
