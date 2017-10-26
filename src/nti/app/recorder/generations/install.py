#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

from zope import interface

from zope.generations.generations import SchemaManager as BaseSchemaManager

from zope.generations.interfaces import IInstallableSchemaManager

from zope.intid.interfaces import IIntIds

from nti.recorder.index import install_recorder_catalog
from nti.recorder.index import install_transaction_catalog

generation = 4

logger = __import__('logging').getLogger(__name__)


@interface.implementer(IInstallableSchemaManager)
class _SchemaManager(BaseSchemaManager):

    def __init__(self):
        super(_SchemaManager, self).__init__(
            generation=generation,
            minimum_generation=generation,
            package_name='nti.app.recorder.generations')

    def install(self, context):
        evolve(context)


def evolve(context):
    install_catalog(context)


def install_catalog(context):
    conn = context.connection
    root = conn.root()
    dataserver_folder = root['nti.dataserver']
    lsm = dataserver_folder.getSiteManager()
    intids = lsm.getUtility(IIntIds)
    install_recorder_catalog(dataserver_folder, intids)
    install_transaction_catalog(dataserver_folder, intids)
