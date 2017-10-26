#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

import os
import sys
import pprint
import argparse

from zope import component

from nti.app.recorder.utils.catalog import RebuildRecorderCatalog
from nti.app.recorder.utils.catalog import RebuildTransactionCatalog

from nti.dataserver.utils import run_with_dataserver

from nti.dataserver.utils.base_script import create_context

from nti.externalization.interfaces import StandardExternalFields

ITEMS = StandardExternalFields.ITEMS
TOTAL = StandardExternalFields.TOTAL
ITEM_COUNT = StandardExternalFields.ITEM_COUNT

logger = __import__('logging').getLogger(__name__)


def _sync_library():
    try:
        from nti.contentlibrary.interfaces import IContentPackageLibrary
        library = component.getUtility(IContentPackageLibrary)
        if library is not None:
            library.syncContentPackages()
    except ImportError:
        pass


def _process_args(args):
    _sync_library()
    if args.recorder:
        logger.info("Rebuilding recorder catalog...")
        builder = RebuildRecorderCatalog()
    elif args.transaction:
        logger.info("Rebuilding transaction catalog...")
        builder = RebuildTransactionCatalog()
    else:
        builder = None
    if builder is None:
        return
    seen = set()
    items = builder(seen, args.metadata)
    if args.verbose:
        result = {
            ITEMS: items,
            TOTAL: len(seen),
            ITEM_COUNT: len(seen)
        }
        pprint.pprint(result)


def main():
    arg_parser = argparse.ArgumentParser(description="Rebuild recorder catalog")
    arg_parser.add_argument('-v', '--verbose', help="Be Verbose", action='store_true',
                            dest='verbose')

    arg_parser.add_argument('-m', '--metadata',
                            help="Index objects in metadata catalog",
                            action='store_true',
                            dest='metadata')

    site_group = arg_parser.add_mutually_exclusive_group()
    site_group.add_argument('-t', '--transaction',
                            dest='transaction',
                            action='store_true',
                            help="Rebuild transaction catalog")

    site_group.add_argument('-r', '--recorder',
                            dest='recorder',
                            action='store_true',
                            help="Rebuild recorder catalog")

    args = arg_parser.parse_args()
    env_dir = os.getenv('DATASERVER_DIR')
    if not env_dir or not os.path.exists(env_dir) and not os.path.isdir(env_dir):
        raise IOError("Invalid dataserver environment root directory")

    conf_packages = ('nti.appserver',)
    context = create_context(env_dir, with_library=True)

    run_with_dataserver(environment_dir=env_dir,
                        verbose=args.verbose,
                        xmlconfig_packages=conf_packages,
                        context=context,
                        minimal_ds=True,
                        function=lambda: _process_args(args))
    sys.exit(0)


if __name__ == '__main__':
    main()
