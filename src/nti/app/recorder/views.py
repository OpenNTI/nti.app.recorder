#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

from requests.structures import CaseInsensitiveDict

from pyramid import httpexceptions as hexc

from pyramid.view import view_config
from pyramid.view import view_defaults

from zope import component
from zope import lifecycleevent

from zope.intid.interfaces import IIntIds

from nti.app.base.abstract_views import AbstractAuthenticatedView

from nti.app.externalization.error import raise_json_error

from nti.app.externalization.view_mixins import BatchingUtilsMixin
from nti.app.externalization.view_mixins import ModeledContentUploadRequestUtilsMixin

from nti.app.recorder import MessageFactory as _

from nti.app.recorder.utils import parse_datetime

from nti.appserver.pyramid_authorization import has_permission

from nti.dataserver.authorization import ACT_UPDATE
from nti.dataserver.authorization import ACT_CONTENT_EDIT

from nti.dataserver.interfaces import IUser

from nti.externalization.interfaces import LocatedExternalDict
from nti.externalization.interfaces import StandardExternalFields

from nti.recorder.index import IX_PRINCIPAL
from nti.recorder.index import IX_CREATEDTIME
from nti.recorder.index import get_transaction_catalog

from nti.recorder.interfaces import IRecordable
from nti.recorder.interfaces import ITransactionRecord
from nti.recorder.interfaces import IRecordableContainer
from nti.recorder.interfaces import ITransactionRecordHistory

from nti.recorder.record import get_transactions
from nti.recorder.record import remove_transaction_history

ITEMS = StandardExternalFields.ITEMS
TOTAL = StandardExternalFields.TOTAL
ITEM_COUNT = StandardExternalFields.ITEM_COUNT

logger = __import__('logging').getLogger(__name__)


class AbstractRecordableObjectView(AbstractAuthenticatedView):

    def _chek_perms(self):
        if not (has_permission(ACT_UPDATE, self.context, self.request)
                or has_permission(ACT_CONTENT_EDIT, self.context, self.request)):
            raise hexc.HTTPForbidden()

    def _do_call(self):
        pass

    def __call__(self):
        self._chek_perms()
        return self._do_call()


@view_config(route_name='objects.generic.traversal',
             renderer='rest',
             request_method='POST',
             context=IRecordable,
             name='SyncLock')
class SyncLockObjectView(AbstractRecordableObjectView):

    def _do_call(self):
        # pylint: disable=no-member
        self.context.lock()
        lifecycleevent.modified(self.context)
        return self.context


@view_config(route_name='objects.generic.traversal',
             renderer='rest',
             request_method='POST',
             context=IRecordable,
             name='SyncUnlock')
class SyncUnlockObjectView(AbstractRecordableObjectView):

    def _do_call(self):
        # pylint: disable=no-member
        self.context.unlock()
        lifecycleevent.modified(self.context)
        return self.context


@view_config(route_name='objects.generic.traversal',
             renderer='rest',
             request_method='POST',
             context=IRecordableContainer,
             name='ChildOrderLock')
class ChildOrderLockObjectView(AbstractRecordableObjectView):

    def _do_call(self):
        # pylint: disable=no-member
        self.context.childOrderLock()
        lifecycleevent.modified(self.context)
        return self.context


@view_config(route_name='objects.generic.traversal',
             renderer='rest',
             request_method='POST',
             context=IRecordableContainer,
             name='ChildOrderUnlock')
class ChildOrderUnlockObjectView(AbstractRecordableObjectView):

    def _do_call(self):
        # pylint: disable=no-member
        self.context.childOrderUnlock()
        lifecycleevent.modified(self.context)
        return self.context


@view_config(route_name='objects.generic.traversal',
             renderer='rest',
             request_method='GET',
             context=IRecordable,
             name='SyncLockStatus')
class SyncLockObjectStatusView(AbstractRecordableObjectView):

    def _do_call(self):
        # pylint: disable=no-member
        result = LocatedExternalDict()
        result['Locked'] = self.context.isLocked()
        if IRecordableContainer.providedBy(self.context):
            result['ChildOrderLocked'] = self.context.isChildOrderLocked()
        return result


@view_config(name='audit_log')
@view_config(name='TransactionHistory')
@view_defaults(route_name='objects.generic.traversal',
               renderer='rest',
               request_method='GET',
               context=IRecordable)
class TransactionHistoryView(AbstractRecordableObjectView,
                             BatchingUtilsMixin):

    _DEFAULT_BATCH_SIZE = 20
    _DEFAULT_BATCH_START = 0

    def readInput(self):
        return CaseInsensitiveDict(self.request.params)

    def _do_call(self):
        data = self.readInput()
        endTime = data.get('endTime')
        startTime = data.get('startTime')
        # parse time input
        endTime = parse_datetime(endTime) if endTime else None
        startTime = parse_datetime(startTime) if startTime else None
        # perform query
        result = LocatedExternalDict()
        result.__name__ = self.request.view_name
        result.__parent__ = self.request.context
        history = ITransactionRecordHistory(self.context)
        items = sorted(history.query(start_time=startTime, end_time=endTime))
        result[TOTAL] = result['TotalItemCount'] = len(items)
        self._batch_items_iterable(result, items)
        result[ITEM_COUNT] = len(result[ITEMS])
        return result


@view_config(name='clear_log')
@view_config(name='RemoveTransactionHistory')
@view_defaults(route_name='objects.generic.traversal',
               renderer='rest',
               context=IRecordable,
               request_method='POST')
class RemoveTransactionHistoryView(AbstractRecordableObjectView):

    def __call__(self):
        # pylint: disable=no-member
        result = LocatedExternalDict()
        if IRecordableContainer.providedBy(self.context):
            self.context.child_order_unlock()
        self.context.unlock()
        result[ITEMS] = get_transactions(self.context, sort=True)
        remove_transaction_history(self.context)
        lifecycleevent.modified(self.context)
        return result


@view_config(name='trim_log')
@view_config(name='TrimTransactionHistory')
@view_defaults(route_name='objects.generic.traversal',
               renderer='rest',
               request_method='POST',
               context=IRecordable)
class TrimTransactionHistoryView(AbstractRecordableObjectView,
                                 ModeledContentUploadRequestUtilsMixin):

    def readInput(self, value=None):
        result = super(TrimTransactionHistoryView, self).readInput(value)
        return CaseInsensitiveDict(result)

    def _do_call(self):
        data = self.readInput()
        endTime = data.get('endTime')
        startTime = data.get('startTime')
        if not startTime and not endTime:
            raise_json_error(self.request,
                             hexc.HTTPUnprocessableEntity,
                             {
                                 'message': _(u"Must specified a time range."),
                                 'code': 'InvalidTimeRange'
                             },
                             None)
        # parse time input
        endTime = parse_datetime(endTime) if endTime else None
        startTime = parse_datetime(startTime) if startTime else None
        # query history
        history = ITransactionRecordHistory(self.context)
        items = history.query(start_time=startTime, end_time=endTime)
        # remove records
        for item in items:
            # pylint: disable=too-many-function-args
            history.remove(item)
        # return
        result = LocatedExternalDict()
        result[ITEMS] = items
        result[TOTAL] = result[ITEM_COUNT] = len(items)
        return result


@view_defaults(route_name='objects.generic.traversal',
               renderer='rest',
               request_method='GET',
               context=ITransactionRecord)
class TransactionRecordGetView(AbstractRecordableObjectView):

    def _do_call(self):
        return self.context


@view_config(route_name="objects.generic.traversal",
             context=ITransactionRecord,
             renderer='rest',
             request_method='DELETE')
class TransactionRecordDeleteView(AbstractRecordableObjectView):

    def _do_call(self):
        # pylint: disable=no-member
        del self.context.__parent__[self.context.__name__]
        result = hexc.HTTPNoContent()
        return result


@view_config(name='audit_log')
@view_config(name='TransactionHistory')
@view_defaults(route_name='objects.generic.traversal',
               renderer='rest',
               request_method='GET',
               context=IUser,
               permission=ACT_UPDATE)
class UserTransactionHistoryView(AbstractAuthenticatedView,
                                 BatchingUtilsMixin):

    _DEFAULT_BATCH_SIZE = 20
    _DEFAULT_BATCH_START = 0

    def readInput(self):
        return CaseInsensitiveDict(self.request.params)

    def __call__(self):
        values = self.readInput()
        intids = component.getUtility(IIntIds)
        endTime = values.get('endTime') or values.get('endDate')
        startTime = values.get('startTime') or values.get('startDate')
        endTime = parse_datetime(endTime) if endTime else None
        startTime = parse_datetime(startTime) if startTime else None

        result = LocatedExternalDict()
        result.__name__ = self.request.view_name
        result.__parent__ = self.request.context
        items = result[ITEMS] = []

        # query catalog
        # pylint: disable=no-member
        catalog = get_transaction_catalog()
        query = {
            IX_PRINCIPAL: {'any_of': (self.context.username,)},
            IX_CREATEDTIME: {'between': (startTime, endTime)}
        }
        for doc_id in catalog.apply(query) or ():
            obj = intids.queryObject(doc_id)
            if ITransactionRecord.providedBy(obj):
                items.append(obj)

        # sort & batch
        items.sort(key=lambda x: x.createdTime, reverse=True)
        result[TOTAL] = result['TotalItemCount'] = len(items)
        self._batch_items_iterable(result, items)
        result[ITEM_COUNT] = len(result[ITEMS])
        return result
