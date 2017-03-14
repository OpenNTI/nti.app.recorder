#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

from requests.structures import CaseInsensitiveDict

from zope import lifecycleevent

from pyramid import httpexceptions as hexc

from pyramid.view import view_config
from pyramid.view import view_defaults

from nti.app.base.abstract_views import AbstractAuthenticatedView

from nti.app.externalization.error import raise_json_error

from nti.app.externalization.view_mixins import ModeledContentUploadRequestUtilsMixin

from nti.app.recorder import MessageFactory as _

from nti.app.recorder.utils import parse_datetime

from nti.appserver.pyramid_authorization import has_permission

from nti.coremetadata.interfaces import IRecordable
from nti.coremetadata.interfaces import IRecordableContainer

from nti.dataserver.authorization import ACT_UPDATE
from nti.dataserver.authorization import ACT_CONTENT_EDIT

from nti.externalization.interfaces import LocatedExternalDict
from nti.externalization.interfaces import StandardExternalFields

from nti.recorder.interfaces import ITransactionRecord
from nti.recorder.interfaces import ITransactionRecordHistory

ITEMS = StandardExternalFields.ITEMS
TOTAL = StandardExternalFields.TOTAL
ITEM_COUNT = StandardExternalFields.ITEM_COUNT


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
class TransactionHistoryView(AbstractRecordableObjectView):

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
        history = ITransactionRecordHistory(self.context)
        items = sorted(history.query(start_time=startTime, end_time=endTime))
        result[TOTAL] = result[ITEM_COUNT] = len(items)
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
                                 'message': _("Must specified a time range."),
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
            history.remove(item)
        # return
        result = LocatedExternalDict()
        result[ITEMS] = items
        result[TOTAL] = result[ITEM_COUNT] = len(items)
        return result


@view_defaults(route_name='objects.generic.traversal',
               renderer='rest',
               request_method='DELETE',
               context=ITransactionRecord)
class TransactionRecordDeleteView(AbstractAuthenticatedView):

    def __call__(self):
        del self.context.__parent__[self.context.__name__]
        result = hexc.HTTPNoContent()
        return result
