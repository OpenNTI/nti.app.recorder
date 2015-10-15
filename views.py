#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

from pyramid import httpexceptions as hexc

from pyramid.view import view_config
from pyramid.view import view_defaults

from nti.app.base.abstract_views import AbstractAuthenticatedView

from nti.coremetadata.interfaces import IRecordable

from nti.recorder import get_transactions
from nti.recorder import remove_transaction_history

from nti.dataserver.authorization import ACT_NTI_ADMIN

from nti.externalization.interfaces import LocatedExternalDict
from nti.externalization.interfaces import StandardExternalFields

ITEMS = StandardExternalFields.ITEMS

@view_config(permission=ACT_NTI_ADMIN)
@view_defaults(route_name='objects.generic.traversal',
			   renderer='rest',
			   request_method='POST',
			   context=IRecordable,
			   name='RemoveTransactionHistory')
class RemoveTransactionHistoryView(AbstractAuthenticatedView):

	def __call__(self):
		remove_transaction_history(self.context)
		return hexc.HTTPNoContent()

@view_config(permission=ACT_NTI_ADMIN)
@view_defaults(route_name='objects.generic.traversal',
			   renderer='rest',
			   request_method='POST',
			   context=IRecordable,
			   name='TransactionHistory')
class TransactionHistoryView(AbstractAuthenticatedView):

	def __call__(self):
		result = LocatedExternalDict()
		result[ITEMS] = get_transactions(self.context)
		return result
