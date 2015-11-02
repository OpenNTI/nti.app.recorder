#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

from zope import lifecycleevent

from pyramid.view import view_config
from pyramid.view import view_defaults
from pyramid import httpexceptions as hexc

from nti.app.base.abstract_views import AbstractAuthenticatedView

from nti.coremetadata.interfaces import IRecordable

from nti.dataserver.authorization import ACT_UPDATE

from nti.externalization.interfaces import LocatedExternalDict
from nti.externalization.interfaces import StandardExternalFields

from nti.recorder import get_transactions

ITEMS = StandardExternalFields.ITEMS

@view_config(permission=ACT_UPDATE)
@view_defaults(route_name='objects.generic.traversal',
			   renderer='rest',
			   request_method='POST',
			   context=IRecordable,
			   name='SyncUnlock')
class SyncUnlockObjectView(AbstractAuthenticatedView):

	def __call__(self):
		self.context.locked = False
		lifecycleevent.modified(self.context)
		return hexc.HTTPNoContent()

@view_config(permission=ACT_UPDATE)
@view_defaults(route_name='objects.generic.traversal',
			   renderer='rest',
			   request_method='POST',
			   context=IRecordable,
			   name='SyncLock')
class SyncLockObjectView(AbstractAuthenticatedView):

	def __call__(self):
		self.context.locked = True
		lifecycleevent.modified(self.context)
		return hexc.HTTPNoContent()

@view_config(name='audit_log')
@view_config(name='TransactionHistory')
@view_defaults(route_name='objects.generic.traversal',
			   renderer='rest',
			   request_method='GET',
			   permission=ACT_UPDATE,
			   context=IRecordable)
class TransactionHistoryView(AbstractAuthenticatedView):

	def __call__(self):
		result = LocatedExternalDict()
		result[ITEMS] = get_transactions(self.context, sort=True)
		return result
