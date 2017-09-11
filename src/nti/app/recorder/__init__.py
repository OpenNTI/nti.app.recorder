#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import print_function, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import zope.i18nmessageid
MessageFactory = zope.i18nmessageid.MessageFactory('nti.dataserver')

from zope import interface

from zope.location.interfaces import IContained

from zope.traversing.interfaces import IPathAdapter

VIEW_RECURSIVE_AUDIT_LOG = 'recursive_audit_log'
VIEW_RECURSIVE_TX_HISTORY = 'RecursiveTransactionHistory'

#: Recorder path adapter
RECORDER_ADAPTER = 'recorder'


@interface.implementer(IPathAdapter, IContained)
class RecorderPathAdapter(object):

    __name__ = RECORDER_ADAPTER

    def __init__(self, context, request):
        self.context = context
        self.request = request
        self.__parent__ = context