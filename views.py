#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

from zope import lifecycleevent

from pyramid import httpexceptions as hexc

from pyramid.view import view_config
from pyramid.view import view_defaults

from nti.app.base.abstract_views import AbstractAuthenticatedView

from nti.appserver.pyramid_authorization import has_permission

from nti.coremetadata.interfaces import IRecordable
from nti.coremetadata.interfaces import IRecordableContainer

from nti.dataserver.authorization import ACT_UPDATE
from nti.dataserver.authorization import ACT_CONTENT_EDIT

from nti.externalization.interfaces import LocatedExternalDict
from nti.externalization.interfaces import StandardExternalFields

from nti.recorder import get_transactions

ITEMS = StandardExternalFields.ITEMS
TOTAL = StandardExternalFields.TOTAL
ITEM_COUNT = StandardExternalFields.ITEM_COUNT

class AbstractRecordableObjectView(AbstractAuthenticatedView):

	def _chek_perms(self):
		if not (	has_permission(ACT_UPDATE, self.context, self.request) \
				or	has_permission(ACT_CONTENT_EDIT, self.context, self.request) ):
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
		return self.contextl

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

	def _do_call(self):
		result = LocatedExternalDict()
		items = result[ITEMS] = get_transactions(self.context, sort=True)
		result[TOTAL] = result[ITEM_COUNT] = len(items)
		return result
