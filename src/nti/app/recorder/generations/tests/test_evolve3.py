#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# pylint: disable=protected-access,too-many-public-methods

from hamcrest import is_
from hamcrest import none
from hamcrest import assert_that

import unittest

import fudge

from zc.catalog.index import ValueIndex

from zope import component

from zope.intid.interfaces import IIntIds

from nti.app.recorder.generations import evolve3

from nti.app.recorder.tests import SharedConfiguringTestLayer

from nti.dataserver.tests import mock_dataserver

from nti.recorder.index import IX_RECORDABLE
from nti.recorder.index import get_transaction_catalog

from nti.recorder.record import TransactionRecord


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
