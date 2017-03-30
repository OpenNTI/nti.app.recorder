#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

generation = 2

from zope import component
from zope import interface

from zope.component.hooks import site
from zope.component.hooks import setHooks

from zope.index.topic import TopicIndex

from zope.index.topic.interfaces import ITopicFilteredSet

from zope.intid.interfaces import IIntIds

from zc.catalog.interfaces import IIndexValues

from nti.dataserver.interfaces import IDataserver
from nti.dataserver.interfaces import IOIDResolver

from nti.recorder.index import IX_TID
from nti.recorder.index import IX_TYPE
from nti.recorder.index import IX_PRINCIPAL
from nti.recorder.index import IX_ATTRIBUTES
from nti.recorder.index import IX_CREATEDTIME

from nti.recorder.index import install_recorder_catalog
from nti.recorder.index import install_transaction_catalog

from nti.recorder.interfaces import IRecordable
from nti.recorder.interfaces import ITransactionRecord

from nti.zope_catalog.interfaces import IKeywordIndex


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


def _get_ids(catalog):
    result = set()
    for name, index in catalog.items():
        try:
            if IIndexValues.providedBy(index):
                result.update(index.ids())
            elif IKeywordIndex.providedBy(index):
                result.update(index.ids())
            elif isinstance(index, TopicIndex):
                for filter_index in index._filters.values():
                    if ITopicFilteredSet.providedBy(filter_index):
                        result.update(filter_index.getIds())
        except Exception:
            logger.error('Errors getting ids from index "%s" (%s) in catalog %s',
                         name, index, catalog)
    return result


def do_evolve(context):
    setHooks()
    conn = context.connection
    root = conn.root()
    ds_folder = root['nti.dataserver']

    mock_ds = MockDataserver()
    mock_ds.root = ds_folder
    component.provideUtility(mock_ds, IDataserver)

    with site(ds_folder):
        assert component.getSiteManager() == ds_folder.getSiteManager(), \
               "Hooks not installed?"

        lsm = ds_folder.getSiteManager()
        intids = lsm.getUtility(IIntIds)

        # install catalogs
        recorder = install_recorder_catalog(ds_folder, intids)
        transaction = install_transaction_catalog(ds_folder, intids)

        # get all ids
        all_ids = _get_ids(recorder)
        recorder.clear()  # clear all

        # unregister columns
        for name in (IX_TID, IX_TYPE, IX_PRINCIPAL, IX_ATTRIBUTES, IX_CREATEDTIME):
            try:
                index = recorder[name]
                intids.unregister(index)
                del recorder[name]
                index.__parent__ = None
            except KeyError:
                pass

        c_rec = c_trx = 0
        for doc_id in all_ids:
            obj = intids.queryObject(doc_id)
            if ITransactionRecord.providedBy(obj):
                transaction.index_doc(doc_id, obj)
                c_trx += 1
            elif IRecordable.providedBy(obj):
                recorder.index_doc(doc_id, obj)
                c_rec += 1

    component.getGlobalSiteManager().unregisterUtility(mock_ds, IDataserver)
    logger.info('Dataserver evolution %s done. %s recordable(s), %s record(s)',
                generation, c_rec, c_trx)


def evolve(context):
    """
    Evolve to gen 2 by creating a trx record index
    """
    do_evolve(context)
