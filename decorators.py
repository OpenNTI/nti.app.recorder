#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

from zope import component
from zope import interface

from nti.app.renderers.decorators import AbstractAuthenticatedRequestAwareDecorator

from nti.appserver.pyramid_authorization import has_permission

from nti.coremetadata.interfaces import IRecordable

from nti.dataserver.authorization import ACT_UPDATE

from nti.externalization.externalization import to_external_object
from nti.externalization.interfaces import IExternalMappingDecorator

from nti.recorder.interfaces import ITransactionRecord

@component.adapter(ITransactionRecord)
@interface.implementer(IExternalMappingDecorator)
class _TransactionRecordDecorator(AbstractAuthenticatedRequestAwareDecorator):

	def _do_decorate_external(self, context, result):
		target = context.__parent__
		if target is not None:
			result['Target'] = to_external_object(target)

@component.adapter(IRecordable)
@interface.implementer(IExternalMappingDecorator)
class _RecordableDecorator(AbstractAuthenticatedRequestAwareDecorator):

	def _predicate(self, context, result):
		return 	bool(self.authenticated_userid) and \
				has_permission(ACT_UPDATE, context, self.request)

	def _do_decorate_external(self, context, result):
		result['locked'] = context.locked
