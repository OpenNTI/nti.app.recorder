#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import print_function, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

generation = 3

from zope import component
from zope import interface

from zope.component.hooks import site
from zope.component.hooks import setHooks

from zope.intid.interfaces import IIntIds

from nti.dataserver.interfaces import IDataserver
from nti.dataserver.interfaces import IOIDResolver

from nti.recorder.index import IX_RECORDABLE

from nti.recorder.index import get_transaction_catalog

from nti.recorder.interfaces import ITransactionRecord

from nti.zodb import isBroken


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


def process_meta(obj):
    try:
        from nti.metadata import queue_removed
        queue_removed(obj)
    except ImportError:
        pass

  
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
        catalog = get_transaction_catalog(lsm)
        index = catalog[IX_RECORDABLE]
        for rec_id, trxs in list(index.values_to_documents.items()):
            recordable = intids.queryObject(rec_id)
            if recordable is None or isBroken(recordable):
                for trx_id in tuple(trxs):
                    count += 1
                    transaction = intids.queryObject(trx_id)
                    if ITransactionRecord.providedBy(transaction):
                        process_meta(transaction)
                        transaction.__parent__ = None
                    try:
                        intids.force_unregister(trx_id, transaction)
                    except KeyError:
                        pass
                    catalog.unindex_doc(trx_id)

    component.getGlobalSiteManager().unregisterUtility(mock_ds, IDataserver)
    logger.info('Evolution %s done. %s record(s) unregistered', 
                generation, count)
    return count


def evolve(context):
    """
    Evolve to gen 3 by unregistering leaked transaction records
    """
    do_evolve(context)
