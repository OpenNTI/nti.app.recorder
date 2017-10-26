#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

from zope import component
from zope import interface

from zope.component.hooks import site
from zope.component.hooks import setHooks

from zope.intid.interfaces import IIntIds

from nti.dataserver.interfaces import IDataserver
from nti.dataserver.interfaces import IOIDResolver

from nti.recorder.index import IX_LOCKED
from nti.recorder.index import IX_MIMETYPE
from nti.recorder.index import IX_CHILD_ORDER_LOCKED

from nti.recorder.index import get_recorder_catalog

from nti.zodb import isBroken

generation = 4

logger = __import__('logging').getLogger(__name__)


@interface.implementer(IDataserver)
class MockDataserver(object):

    root = None

    def get_by_oid(self, oid, ignore_creator=False):
        resolver = component.queryUtility(IOIDResolver)
        if resolver is None:
            logger.warn("Using dataserver without a proper ISiteManager.")
        else:
            return resolver.get_object_by_oid(oid, ignore_creator=ignore_creator)
        return None


def do_evolve(context):
    setHooks()
    conn = context.connection
    root = conn.root()
    ds_folder = root['nti.dataserver']

    count = 0
    mock_ds = MockDataserver()
    mock_ds.root = ds_folder
    component.provideUtility(mock_ds, IDataserver)

    with site(ds_folder):
        assert component.getSiteManager() == ds_folder.getSiteManager(), \
               "Hooks not installed?"

        lsm = ds_folder.getSiteManager()
        intids = lsm.getUtility(IIntIds)
        catalog = get_recorder_catalog(lsm)

        documents = set()
        for name in (IX_LOCKED, IX_CHILD_ORDER_LOCKED):
            index = catalog[name]
            documents.update(index.documents_to_values.keys())

        index = catalog[IX_MIMETYPE]
        index.clear()
        for doc_id in documents:
            recordable = intids.queryObject(doc_id)
            if recordable is None or isBroken(recordable):
                catalog.unindex_doc(doc_id)
            else:
                count += 1
                index.index_doc(doc_id, recordable)

    component.getGlobalSiteManager().unregisterUtility(mock_ds, IDataserver)
    logger.info('Evolution %s done. %s record(s) indexed',
                generation, count)
    return count


def evolve(context):
    """
    Evolve to gen 4 by indexing the mime-types of recordable objects
    """
    do_evolve(context)
