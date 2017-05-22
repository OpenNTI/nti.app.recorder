#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import print_function, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

from zope import component
from zope import interface

from zope.cachedescriptors.property import Lazy

from zope.intid.interfaces import IIntIds

from zope.location.interfaces import ILocation

from zope.mimetype.interfaces import IContentTypeAware

from ZODB.interfaces import IConnection

from nti.app.renderers.decorators import AbstractAuthenticatedRequestAwareDecorator

from nti.appserver.pyramid_authorization import has_permission

from nti.dataserver.authorization import ACT_UPDATE

from nti.externalization.interfaces import StandardExternalFields
from nti.externalization.interfaces import IExternalMappingDecorator

from nti.links.links import Link

from nti.recorder.interfaces import IRecordable
from nti.recorder.interfaces import ITransactionRecord
from nti.recorder.interfaces import IRecordableContainer

from nti.recorder.utils import decompress

from nti.traversal.traversal import find_interface

CLASS = StandardExternalFields.CLASS
INTID = StandardExternalFields.INTID
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
        # gather some minor info
        if intids is not None and recordable is not None:
            ntiid =  getattr(recordable, NTIID.lower(), None) \
                  or getattr(recordable, NTIID, None)
            clazz =  getattr(recordable, '__external_class_name__', None) \
                  or recordable.__class__.__name__

            aware = IContentTypeAware(recordable, recordable)
            mimeType = getattr(aware, 'mimeType', None) \
                    or getattr(aware, 'mime_type', None)
            title = getattr(recordable, 'title', None) \
                 or getattr(recordable, 'label', None)
            result['Recordable'] = {
                CLASS: clazz,
                NTIID: ntiid,
                MIMETYPE: mimeType,
                INTID: intids.queryId(recordable),
                'Title': title
            }


@component.adapter(IRecordable)
@interface.implementer(IExternalMappingDecorator)
class _RecordableDecorator(AbstractAuthenticatedRequestAwareDecorator):

    @Lazy
    def _acl_decoration(self):
        return getattr(self.request, 'acl_decoration', True)

    @Lazy
    def intids(self):
        return component.getUtility(IIntIds)

    def _predicate(self, context, result):
        """
        Only persistent objects for users that have permission.
        """
        return (      self._acl_decoration
                and bool(self.authenticated_userid)
                # Some objects have intids, but do not have connections (?).
                and (   IConnection(context, None) is not None
                     or getattr(context, self.intids.attribute, None))
                and has_permission(ACT_UPDATE, context, self.request))

    def _do_decorate_external(self, context, result):
        added = []
        _links = result.setdefault(LINKS, [])

        # lock/unlock
        if not context.isLocked():
            link = Link(context,
                        rel='SyncLock',
                        elements=('@@SyncLock',))
        else:
            link = Link(context,
                        rel='SyncUnlock',
                        elements=('@@SyncUnlock',))
        added.append(link)

        if IRecordableContainer.providedBy(context):
            if not context.isChildOrderLocked():
                link = Link(context,
                            method='POST',
                            rel='ChildOrderLock',
                            elements=('@@ChildOrderLock',))
            else:
                link = Link(context,
                            method='POST',
                            rel='ChildOrderUnlock',
                            elements=('@@ChildOrderUnlock',))
            added.append(link)

        # audit log
        link = Link(context,
                    rel='audit_log',
                    elements=('@@audit_log',))
        added.append(link)
        
        for name in ('trim_log', 'clear_log'):
            link = Link(context,
                        method='POST',
                        rel=name,
                        elements=('@@%s' % name,))
            added.append(link)

        # add links
        for link in added:
            interface.alsoProvides(link, ILocation)
            link.__name__ = ''
            link.__parent__ = context
            _links.append(link)
