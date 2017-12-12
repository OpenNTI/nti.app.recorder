#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

from ZODB.POSException import POSError

from zope import component

from zope.cachedescriptors.property import Lazy

from zope.component.hooks import site as current_site

from zope.intid.interfaces import IIntIds

from nti.dataserver.metadata.index import get_metadata_catalog

from nti.recorder.index import get_recorder_catalog
from nti.recorder.index import get_transaction_catalog

from nti.recorder.interfaces import get_recordables

from nti.recorder.record import get_transactions
from nti.recorder.record import has_transactions

from nti.site.hostpolicy import get_all_host_sites

logger = __import__('logging').getLogger(__name__)


class RebuildCatalogMixin(object):

    def _catalog(self):
        raise NotImplementedError

    def _indexables(self, recordable):
        raise NotImplementedError

    @Lazy
    def metadata_catalog(self):
        return get_metadata_catalog()

    def __call__(self, seen=None, metadata=True):
        intids = component.getUtility(IIntIds)
        # remove indexes
        catalog = self._catalog()
        for index in catalog.values():
            index.clear()
        # reindex
        items = dict()
        seen = set() if seen is None else seen
        for host_site in get_all_host_sites():  # check all sites
            with current_site(host_site):
                count = 0
                for recordable in get_recordables():
                    for indexable in self._indexables(recordable):
                        doc_id = intids.queryId(indexable)
                        if doc_id is None or doc_id in seen:
                            continue
                        seen.add(doc_id)
                        try:
                            catalog.index_doc(doc_id, indexable)
                            if metadata:
                                # pylint: disable=no-member
                                self.metadata_catalog.index_doc(doc_id, indexable)
                        except POSError:
                            logger.error("Error while indexing object %s/%s",
                                         doc_id, type(recordable))
                        else:
                            count += 1
                logger.info("%s object(s) indexed in site %s",
                            count, host_site.__name__)
                items[host_site.__name__] = count
        return items


class RebuildTransactionCatalog(RebuildCatalogMixin):

    def _catalog(self):
        return get_transaction_catalog()

    def _indexables(self, recordable):
        if has_transactions(recordable):
            return get_transactions(recordable) 
        return ()


class RebuildRecorderCatalog(RebuildCatalogMixin):

    def _catalog(self):
        return get_recorder_catalog()

    def _indexables(self, recordable):
        return (recordable,)
