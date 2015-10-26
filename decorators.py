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

from nti.coremetadata.interfaces import IRecordable

from nti.dataserver.authorization import ACT_UPDATE

from nti.externalization.interfaces import StandardExternalFields
from nti.externalization.interfaces import IExternalMappingDecorator

from nti.externalization.externalization import to_external_object

from nti.links.links import Link

from nti.recorder.interfaces import ITransactionRecord

LINKS = StandardExternalFields.LINKS

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
		_links = result.setdefault(LINKS, [])
		if not context.locked:
			link = Link(context, rel='lock', elements=('Lock',))
		else:
			link = Link(context, rel='Unlock', elements=('Unlock',))
		interface.alsoProvides(link, ILocation)
		link.__name__ = ''
		link.__parent__ = context
		_links.append(link)
		_links.append(link)
