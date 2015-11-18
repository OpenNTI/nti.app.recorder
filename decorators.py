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

from zope.location.interfaces import ILocation

from nti.app.renderers.decorators import AbstractAuthenticatedRequestAwareDecorator

from nti.appserver.pyramid_authorization import has_permission

from nti.common.property import Lazy

from nti.coremetadata.interfaces import IRecordable

from nti.dataserver.authorization import ACT_UPDATE

from nti.externalization.interfaces import StandardExternalFields
from nti.externalization.interfaces import IExternalMappingDecorator

from nti.links.links import Link

from nti.recorder.utils import decompress
from nti.recorder.interfaces import ITransactionRecord

LINKS = StandardExternalFields.LINKS

@component.adapter(ITransactionRecord)
@interface.implementer(IExternalMappingDecorator)
class _TransactionRecordDecorator(AbstractAuthenticatedRequestAwareDecorator):

	def _do_decorate_external(self, context, result):
		ext_value = context.external_value
		if ext_value is not None:
			try:
				result['ExternalValue'] = decompress(ext_value)
			except Exception:
				pass

@component.adapter(IRecordable)
@interface.implementer(IExternalMappingDecorator)
class _RecordableDecorator(AbstractAuthenticatedRequestAwareDecorator):

	@Lazy
	def _no_acl_decoration_in_request(self):
		request = self.request
		result = getattr(request, 'no_acl_decoration', False)
		return result

	def _predicate(self, context, result):
		return 	not self._no_acl_decoration_in_request and \
				bool(self.authenticated_userid) and \
				has_permission(ACT_UPDATE, context, self.request)

	def _do_decorate_external(self, context, result):
		added = []
		_links = result.setdefault(LINKS, [])
		
		# lock/unlock
		if not context.locked:
			link = Link(context, rel='SyncLock', elements=('@@SyncLock',))
		else:
			link = Link(context, rel='SyncUnlock', elements=('@@SyncUnlock',))
		added.append(link)
		
		# audit log
		link = Link(context, rel='audit_log', elements=('@@audit_log',))
		added.append(link)
		
		# add links
		for link in added:
			interface.alsoProvides(link, ILocation)
			link.__name__ = ''
			link.__parent__ = context
			_links.append(link)
