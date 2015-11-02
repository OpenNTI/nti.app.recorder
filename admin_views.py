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
import isodate
from datetime import date
from datetime import datetime

from zope import component
from zope import lifecycleevent

from zope.intid import IIntIds

from pyramid.view import view_config
from pyramid.view import view_defaults
from pyramid import httpexceptions as hexc

from nti.app.base.abstract_views import AbstractAuthenticatedView

from nti.common.maps import CaseInsensitiveDict

from nti.coremetadata.interfaces import IRecordable

from nti.dataserver.interfaces import IDataserver
from nti.dataserver.interfaces import IShardLayout

from nti.recorder.index import IX_SITE
from nti.recorder.index import IX_PRINCIPAL
from nti.recorder.index import IX_CREATEDTIME

from nti.recorder import get_recorder_catalog
from nti.recorder.record import get_transactions
from nti.recorder.record import remove_transaction_history

from nti.dataserver.authorization import ACT_NTI_ADMIN

from nti.externalization.interfaces import LocatedExternalDict
from nti.externalization.interfaces import StandardExternalFields

from nti.site.site import get_component_hierarchy_names

ITEMS = StandardExternalFields.ITEMS

@view_config(permission=ACT_NTI_ADMIN)
@view_defaults(route_name='objects.generic.traversal',
			   renderer='rest',
			   context=IRecordable,
			   name='RemoveTransactionHistory')
class RemoveTransactionHistoryView(AbstractAuthenticatedView):

	def __call__(self):
		result = LocatedExternalDict()
		self.context.locked = False
		result[ITEMS] = get_transactions(self.context, sort=True)
		remove_transaction_history(self.context)
		lifecycleevent.modified(self.context)
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
			t = isodate.parse_date(t)
		except Exception:
			t = isodate.parse_datetime(t)
	if isinstance(t, (date, datetime)):
		t = time.mktime(t.timetuple())
	if not isinstance(t, float):
		raise ValueError("Invalid date[time]")
	return t

@view_config(permission=ACT_NTI_ADMIN)
@view_defaults(route_name='objects.generic.traversal',
			   renderer='rest',
			   request_method='GET',
			   context=IDataserver,
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

		if not usernames:
			raise hexc.HTTPUnprocessableEntity("Must provide a username")

		startTime = values.get('startTime') or values.get('startDate')
		startTime = parse_datetime(startTime) if startTime is not None else None
		endTime = values.get('endTime') or values.get('endDate')
		endTime = parse_datetime(endTime) if endTime is not None else None

		intids = component.getUtility(IIntIds)
		result = LocatedExternalDict()
		items = result[ITEMS] = {}
		catalog = get_recorder_catalog()
		query = {
			IX_PRINCIPAL:{'any_of':usernames},
			IX_CREATEDTIME:{'between':(startTime, endTime)},
			IX_SITE:{'any_of':get_component_hierarchy_names()},
		}
		for uid in catalog.apply(query) or ():
			context = intids.queryObject(uid)
			if context is None:
				continue
			username = context.principal
			items.setdefault(username, [])
			items[username].append(context)

		for values in items.values():
			values.sort(key=lambda x: x.createdTime)
		return result
