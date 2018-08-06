#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

from zope import component
from zope import interface

from zope.cachedescriptors.property import Lazy

from zope.security.interfaces import IPrincipal

from nti.dataserver.authorization import ROLE_ADMIN

from nti.dataserver.authorization_acl import ace_allowing
from nti.dataserver.authorization_acl import acl_from_aces

from nti.dataserver.interfaces import ALL_PERMISSIONS

from nti.dataserver.interfaces import IACLProvider

from nti.recorder.interfaces import ITransactionRecord

logger = __import__('logging').getLogger(__name__)


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
        aces = [ace_allowing(ROLE_ADMIN, ALL_PERMISSIONS, type(self))]
        creator = IPrincipal(self.context.creator, None)
        if creator is not None:
            aces.append(ace_allowing(creator, ALL_PERMISSIONS, type(self)))
        return acl_from_aces(aces)
