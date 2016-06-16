#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import six
import time
from datetime import date
from datetime import datetime

from zope import component
from zope import lifecycleevent

from zope.interface.common.idatetime import IDate
from zope.interface.common.idatetime import IDateTime

from zope.intid.interfaces import IIntIds

from pyramid.view import view_config
from pyramid.view import view_defaults

from nti.app.base.abstract_views import AbstractAuthenticatedView

from nti.common.maps import CaseInsensitiveDict

from nti.coremetadata.interfaces import IRecordable
from nti.coremetadata.interfaces import IRecordableContainer

from nti.dataserver.authorization import ACT_NTI_ADMIN

from nti.dataserver.interfaces import IDataserver
from nti.dataserver.interfaces import IShardLayout
from nti.dataserver.interfaces import IDataserverFolder

from nti.externalization.interfaces import LocatedExternalDict
from nti.externalization.interfaces import StandardExternalFields

from nti.recorder import get_recorder_catalog

from nti.recorder.index import IX_LOCKED
from nti.recorder.index import IX_PRINCIPAL
from nti.recorder.index import IX_CREATEDTIME
from nti.recorder.index import IX_CHILD_ORDER_LOCKED

from nti.recorder.interfaces import ITransactionRecord

from nti.recorder.record import get_transactions
from nti.recorder.record import remove_transaction_history

from nti.zope_catalog.catalog import ResultSet

ITEMS = StandardExternalFields.ITEMS

def _is_locked(context):
	result = 	(IRecordable.providedBy(context) and context.isLocked()) \
			or	(IRecordableContainer.providedBy(context)
				 and context.is_child_order_locked())
	return result

@view_config(permission=ACT_NTI_ADMIN)
@view_defaults(route_name='objects.generic.traversal',
			   renderer='rest',
			   context=IRecordable,
			   name='RemoveTransactionHistory')
class RemoveTransactionHistoryView(AbstractAuthenticatedView):

	def __call__(self):
		result = LocatedExternalDict()
		if IRecordableContainer.providedBy(self.context):
			self.context.child_order_unlock()
		self.context.unlock()
		result[ITEMS] = get_transactions(self.context, sort=True)
		remove_transaction_history(self.context)
		lifecycleevent.modified(self.context)
		return result

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
			IX_LOCKED:{'any_of':(True,)}
		}
		locked_ids = catalog.apply(query) or catalog.family.IF.LFSet()
		query = {
			IX_CHILD_ORDER_LOCKED:{'any_of':(True,)}
		}
		child_locked_ids = catalog.apply(query) or catalog.family.IF.LFSet()
		doc_ids = catalog.family.IF.multiunion([locked_ids, child_locked_ids])
		for recordable in ResultSet(doc_ids or (), intids, True):
			if 	_is_locked(recordable):
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
class GetLockedObjectsView(AbstractAuthenticatedView):

	def __call__(self):
		result = LocatedExternalDict()
		items = result[ITEMS] = []
		self.request.acl_decoration = False
		intids = component.getUtility(IIntIds)
		catalog = get_recorder_catalog()
		query = {
			IX_LOCKED:{'any_of':(True,)}
		}
		locked_ids = catalog.apply(query) or catalog.family.IF.LFSet()
		query = {
			IX_CHILD_ORDER_LOCKED:{'any_of':(True,)}
		}
		child_locked_ids = catalog.apply(query) or catalog.family.IF.LFSet()
		doc_ids = catalog.family.IF.multiunion([locked_ids, child_locked_ids])
		for context in ResultSet(doc_ids or (), intids, True):
			if _is_locked(context):
				items.append(context)
		result['ItemCount'] = result['Total'] = len(items)
		return result

def _make_min_max_btree_range(search_term):
	min_inclusive = search_term  # start here
	max_exclusive = search_term[0:-1] + unichr(ord(search_term[-1]) + 1)
	return min_inclusive, max_exclusive

def username_search(search_term):
	min_inclusive, max_exclusive = _make_min_max_btree_range(search_term)
	dataserver = component.getUtility(IDataserver)
	_users = IShardLayout(dataserver).users_folder
	usernames = list(_users.iterkeys(min_inclusive, max_exclusive, excludemax=True))
	return usernames

def parse_datetime(t):
	if isinstance(t, six.string_types):
		try:
			t = IDateTime(t)
		except Exception:
			try:
				t = IDate(t)
			except Exception:
				t = float(t)
	if isinstance(t, (date, datetime)):
		t = time.mktime(t.timetuple())
	if not isinstance(t, float):
		raise ValueError("Invalid date[time]")
	return t

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
		usernames = values.get('usernames') or values.get('username')
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
		catalog = get_recorder_catalog()
		query = {
			IX_CREATEDTIME:{'between':(startTime, endTime)}
		}
		if usernames:
			query[IX_PRINCIPAL] = {'any_of':usernames}

		total = 0
		doc_ids = catalog.apply(query)
		for context in ResultSet(doc_ids or (), intids, True):
			if ITransactionRecord.providedBy(context):
				total += 1
				username = context.principal
				items.setdefault(username, [])
				items[username].append(context)

		# add total
		result['Total'] = result['ItemCount'] = total

		# sorted by createdTime
		for values in items.values():
			values.sort(key=lambda x: x.createdTime)
		return result
