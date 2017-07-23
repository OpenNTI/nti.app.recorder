#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, absolute_import, division
__docformat__ = "restructuredtext en"

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import is_
from hamcrest import none
from hamcrest import assert_that

import fudge
import unittest

from zope import component

from zope.intid.interfaces import IIntIds

from zc.catalog.index import ValueIndex

from nti.app.recorder.generations import evolve3

from nti.recorder.index import IX_RECORDABLE
from nti.recorder.index import get_transaction_catalog

from nti.recorder.record import TransactionRecord

from nti.app.recorder.tests import SharedConfiguringTestLayer

from nti.dataserver.tests import mock_dataserver


class TestEvolve3(unittest.TestCase):

    layer = SharedConfiguringTestLayer

    @mock_dataserver.WithMockDSTrans
    def test_installed(self):
        conn = mock_dataserver.current_transaction
        record = TransactionRecord()
        record.username = u'ichigo'
        conn.add(record)
        intids = component.getUtility(IIntIds)
        trx_id = intids.register(record)

        # index
        generated = intids.generateId(None)
        catalog = get_transaction_catalog()
        index = catalog[IX_RECORDABLE]
        ValueIndex.index_doc(index, trx_id, generated)

        context = fudge.Fake().has_attr(connection=conn)
        count = evolve3.do_evolve(context)
        assert_that(count, is_(1))
        assert_that(intids.queryId(record), is_(none()))
