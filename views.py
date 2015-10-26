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

from nti.dataserver.authorization import ACT_UPDATE

@view_config(permission=ACT_UPDATE)
@view_defaults(route_name='objects.generic.traversal',
			   renderer='rest',
			   request_method='POST',
			   context=IRecordable,
			   name='Unlock')
class UnlockObjectView(AbstractAuthenticatedView):

	def __call__(self):
		self.context.locked = False
		return hexc.HTTPNoContent()

@view_config(permission=ACT_UPDATE)
@view_defaults(route_name='objects.generic.traversal',
			   renderer='rest',
			   request_method='POST',
			   context=IRecordable,
			   name='lock')
class LockObjectView(AbstractAuthenticatedView):

	def __call__(self):
		self.context.locked = True
		return hexc.HTTPNoContent()
