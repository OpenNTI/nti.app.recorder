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

from zope.intid.interfaces import IIntIds

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

from nti.traversal.traversal import find_interface

CLASS = StandardExternalFields.CLASS
LINKS = StandardExternalFields.LINKS
NTIID = StandardExternalFields.NTIID
MIMETYPE = StandardExternalFields.MIMETYPE

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
		intids = component.queryUtility(IIntIds)
		recordable = find_interface(context, IRecordable, strict=False)
		if intids is not None and recordable:  # gather some minor info
			ntiid = getattr(recordable, 'ntiid', None) or getattr(recordable, NTIID, None)
			clazz = getattr(recordable, '__external_class_name__', None) or \
					recordable.__class__.__name__
			mimeType = 	getattr(recordable, 'mimeType', None) or \
						getattr(recordable, 'mime_type', None)
			result['Recordable'] = {
				CLASS: clazz,
				NTIID: ntiid,
				MIMETYPE: mimeType,
				'IntId': intids.queryId(recordable)
			}

@component.adapter(IRecordable)
@interface.implementer(IExternalMappingDecorator)
class _RecordableDecorator(AbstractAuthenticatedRequestAwareDecorator):

	@Lazy
	def _acl_decoration(self):
		request = self.request
		result = getattr(request, 'acl_decoration', True)
		return result

	def _predicate(self, context, result):
		"""
		Only persistent objects for users that have permission.
		"""
		return 		self._acl_decoration \
 				and bool(self.authenticated_userid) \
 				and getattr(context, '_p_jar', None) is not None \
				and has_permission(ACT_UPDATE, context, self.request)

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
