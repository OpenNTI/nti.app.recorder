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

from nti.dataserver.authorization_acl import ace_allowing
from nti.dataserver.authorization_acl import acl_from_aces

from nti.dataserver.interfaces import ALL_PERMISSIONS

from nti.dataserver.interfaces import IACLProvider

from nti.recorder.interfaces import ITransactionRecord

from nti.property.property import Lazy


@interface.implementer(IACLProvider)
@component.adapter(ITransactionRecord)
class TransactionRecordACLProvider(object):

    def __init__(self, context):
        self.context = context

    @property
    def __parent__(self):
        return self.context.__parent__

    @Lazy
    def __acl__(self):
        creator = self.context.creator or self.context.principal
        result = acl_from_aces(ace_allowing(creator, ALL_PERMISSIONS, type(self)))
        return result
