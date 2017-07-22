#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import print_function, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

from requests.structures import CaseInsensitiveDict

from zope import component
from zope import lifecycleevent

from zope.index.topic import TopicIndex
from zope.index.topic.interfaces import ITopicFilteredSet

from zope.intid.interfaces import IIntIds

from zc.catalog.interfaces import IIndexValues

from ZODB.POSException import POSError

from pyramid.view import view_config
from pyramid.view import view_defaults

from nti.app.base.abstract_views import AbstractAuthenticatedView

from nti.app.externalization.view_mixins import BatchingUtilsMixin

from nti.app.recorder.utils import parse_datetime

from nti.common.string import is_true

from nti.dataserver.authorization import ACT_NTI_ADMIN

from nti.dataserver.interfaces import IDataserver
from nti.dataserver.interfaces import IShardLayout
from nti.dataserver.interfaces import IDataserverFolder

from nti.externalization.interfaces import LocatedExternalDict
from nti.externalization.interfaces import StandardExternalFields

from nti.recorder.index import IX_LOCKED
from nti.recorder.index import IX_MIMETYPE
from nti.recorder.index import IX_PRINCIPAL
from nti.recorder.index import IX_CREATEDTIME
from nti.recorder.index import IX_CHILD_ORDER_LOCKED

from nti.recorder.index import get_recorder_catalog
from nti.recorder.index import get_transaction_catalog

from nti.recorder.interfaces import IRecordable
from nti.recorder.interfaces import ITransactionRecord
from nti.recorder.interfaces import IRecordableContainer

from nti.recorder.record import remove_transaction_history

from nti.zodb import isBroken

from nti.zope_catalog.interfaces import IKeywordIndex

ITEMS = StandardExternalFields.ITEMS
TOTAL = StandardExternalFields.TOTAL
ITEM_COUNT = StandardExternalFields.ITEM_COUNT


def _is_locked(context):
    result = (IRecordable.providedBy(context) and context.isLocked()) \
          or (  IRecordableContainer.providedBy(context)
              and context.isChildOrderLocked())
    return result


def _resolve_objects(doc_ids, intids):
    for doc_id in doc_ids or ():
        obj = intids.queryObject(doc_id)
        if obj is None or isBroken(obj, doc_id):
            continue
        yield obj


@view_config(permission=ACT_NTI_ADMIN)
@view_defaults(route_name='objects.generic.traversal',
               renderer='rest',
               context=IDataserverFolder,
               name='RemoveAllTransactionHistory')
class RemoveAllTransactionHistoryView(AbstractAuthenticatedView):

    def __call__(self):
        count = 0
        records = 0
        result = LocatedExternalDict()
        catalog = get_recorder_catalog()
        intids = component.getUtility(IIntIds)
        query = {
            IX_LOCKED: {'any_of': (True,)}
        }
        locked_ids = catalog.apply(query) or catalog.family.IF.LFSet()
        query = {
            IX_CHILD_ORDER_LOCKED: {'any_of': (True,)}
        }
        child_locked_ids = catalog.apply(query) or catalog.family.IF.LFSet()
        doc_ids = catalog.family.IF.multiunion([locked_ids, child_locked_ids])
        for recordable in _resolve_objects(doc_ids, intids):
            if _is_locked(recordable):
                count += 1
                recordable.unlock()
                if IRecordableContainer.providedBy(recordable):
                    recordable.child_order_unlock()
                records += remove_transaction_history(recordable)
                lifecycleevent.modified(recordable)
        result['Recordables'] = count
        result['RecordsRemoved'] = records
        return result


@view_config(permission=ACT_NTI_ADMIN)
@view_defaults(route_name='objects.generic.traversal',
               renderer='rest',
               context=IDataserverFolder,
               name='GetLockedObjects')
class GetLockedObjectsView(AbstractAuthenticatedView, BatchingUtilsMixin):

    _DEFAULT_BATCH_START = 0
    _DEFAULT_BATCH_SIZE = 100

    def readInput(self):
        return CaseInsensitiveDict(self.request.params)

    def __call__(self):
        values = self.readInput()
        accept = values.get('accept') or values.get('mimeTypes') or u''
        accept = accept.split(',') if accept else ()
        if accept and '*/*' not in accept:
            accept = {e.strip().lower() for e in accept if e}
            accept.discard('')
        else:
            accept = ()

        links = is_true(values.get('links'))
        self.request.acl_decoration = links

        result = LocatedExternalDict()
        catalog = get_recorder_catalog()
        intids = component.getUtility(IIntIds)

        # query locked
        query = {
            IX_LOCKED: {'any_of': (True,)}
        }
        locked_ids = catalog.apply(query) or catalog.family.IF.LFSet()

        # query child order locked
        query = {
            IX_CHILD_ORDER_LOCKED: {'any_of': (True,)}
        }
        child_locked_ids = catalog.apply(query) or catalog.family.IF.LFSet()
        doc_ids = catalog.family.IF.multiunion([locked_ids, child_locked_ids])

        # query mimeTypes
        if accept:
            query = {
                IX_MIMETYPE: {'any_of': accept}
            }
            mt_ids = catalog.apply(query) or catalog.family.IF.LFSet()
            doc_ids = catalog.family.IF.intersection(doc_ids, mt_ids)

        items = [x for x in _resolve_objects(doc_ids, intids) if _is_locked(x)]
        result[TOTAL] = len(items)
        self._batch_items_iterable(result, items)
        return result


def _make_min_max_btree_range(search_term):
    min_inclusive = search_term  # start here
    max_exclusive = search_term[0:-1] + unichr(ord(search_term[-1]) + 1)
    return min_inclusive, max_exclusive


def username_search(search_term):
    min_inclusive, max_exclusive = _make_min_max_btree_range(search_term)
    dataserver = component.getUtility(IDataserver)
    _users = IShardLayout(dataserver).users_folder
    usernames = _users.iterkeys(min_inclusive,
                                max_exclusive,
                                excludemax=True)
    return list(usernames)


@view_config(permission=ACT_NTI_ADMIN)
@view_defaults(route_name='objects.generic.traversal',
               renderer='rest',
               request_method='GET',
               context=IDataserverFolder,
               name='UserTransactionHistory')
class UserTransactionHistoryView(AbstractAuthenticatedView):

    def __call__(self):
        request = self.request
        values = CaseInsensitiveDict(**request.params)
        term = values.get('term') or values.get('search')
        usernames = values.get('usernames') \
                 or values.get('username') \
                 or values.get('users') \
                 or values.get('user')
        if term:
            usernames = username_search(term)
        elif usernames:
            usernames = usernames.split(",")

        endTime = values.get('endTime') or values.get('endDate')
        startTime = values.get('startTime') or values.get('startDate')
        endTime = parse_datetime(endTime) if endTime is not None else None
        startTime = parse_datetime(startTime) if startTime is not None else None

        intids = component.getUtility(IIntIds)
        result = LocatedExternalDict()
        items = result[ITEMS] = {}
        catalog = get_transaction_catalog()
        query = {
            IX_CREATEDTIME: {'between': (startTime, endTime)}
        }
        if usernames:
            query[IX_PRINCIPAL] = {'any_of': usernames}

        total = 0
        doc_ids = catalog.apply(query)
        for context in _resolve_objects(doc_ids, intids):
            if ITransactionRecord.providedBy(context):
                total += 1
                username = context.principal
                items.setdefault(username, [])
                items[username].append(context)

        # add total
        result[TOTAL] = result[ITEM_COUNT] = total

        # sorted by createdTime
        for values in items.values():
            values.sort(key=lambda x: x.createdTime)
        return result


class RebuildCatalogMixinView(AbstractAuthenticatedView):

    def _catalog(self):
        raise NotImplementedError

    def _process_meta(self, obj):
        try:
            from nti.metadata import queue_add
            queue_add(obj)
        except ImportError:
            pass

    def _get_ids(self, catalog):
        seen = set()
        for name, index in catalog.items():
            try:
                if IIndexValues.providedBy(index):
                    seen.update(index.ids())
                elif IKeywordIndex.providedBy(index):
                    seen.update(index.ids())
                elif isinstance(index, TopicIndex):
                    for filter_index in index._filters.values():
                        if ITopicFilteredSet.providedBy(filter_index):
                            seen.update(filter_index.getIds())
            except (POSError, TypeError):
                logger.error('Errors getting ids from index "%s" (%s)',
                             name, index)
        return seen

    def _get_indexables(self, catalog, intids):
        result = []
        for doc_id in self._get_ids(catalog):
            obj = intids.queryObject(doc_id)
            if obj is None:
                continue
            elif isBroken(obj):
                try:
                    intids.force_unregister(doc_id)
                except KeyError:
                    pass
                continue
            elif IRecordable.providedBy(obj) or ITransactionRecord.providedBy(obj):
                result.append((doc_id, obj))
        return result

    def __call__(self):
        intids = component.getUtility(IIntIds)
        # get indexables and clear indexes
        catalog = self._catalog()
        indexables = self._get_indexables(catalog, intids)
        for index in list(catalog.values()):
            index.clear()
        # reindex
        count = 0
        for doc_id, indexable in indexables:
            count += 1
            catalog.index_doc(doc_id, indexable)
            self._process_meta(indexable)
        result = LocatedExternalDict()
        result[ITEM_COUNT] = result[TOTAL] = count
        return result


@view_config(permission=ACT_NTI_ADMIN)
@view_defaults(route_name='objects.generic.traversal',
               renderer='rest',
               request_method='POST',
               context=IDataserverFolder,
               name='RebuildTransactionCatalog')
class RebuildTransactionCatalogView(RebuildCatalogMixinView):

    def _catalog(self):
        return get_transaction_catalog()


@view_config(permission=ACT_NTI_ADMIN)
@view_defaults(route_name='objects.generic.traversal',
               renderer='rest',
               request_method='POST',
               context=IDataserverFolder,
               name='RebuildRecorderCatalog')
class RebuildRecorderCatalogView(RebuildCatalogMixinView):

    def _catalog(self):
        return get_recorder_catalog()
