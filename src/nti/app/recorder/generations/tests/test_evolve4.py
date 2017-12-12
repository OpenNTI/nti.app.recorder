#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# pylint: disable=protected-access,too-many-public-methods

from hamcrest import is_
from hamcrest import contains
from hamcrest import assert_that

import unittest

import fudge

from persistent import Persistent

from zope import component

from zope.intid.interfaces import IIntIds

from nti.app.recorder.generations import evolve4

from nti.app.recorder.tests import SharedConfiguringTestLayer

from nti.dataserver.tests import mock_dataserver

from nti.recorder.index import IX_MIMETYPE

from nti.recorder.index import get_recorder_catalog

from nti.recorder.mixins import RecordableMixin


class PersistentRecordable(Persistent, RecordableMixin):
    pass


class TestEvolve3(unittest.TestCase):

    layer = SharedConfiguringTestLayer

    @mock_dataserver.WithMockDSTrans
    def test_installed(self):
        conn = mock_dataserver.current_transaction
        record = PersistentRecordable()
        record.locked = True
        conn.add(record)
        intids = component.getUtility(IIntIds)
        doc_id = intids.register(record)

        # index
        catalog = get_recorder_catalog()
        catalog.force_index_doc(doc_id, record)
        # set mimetype and migrate
        record.mimeType = 'foo/foo'  # pylint: disable=attribute-defined-outside-init
        context = fudge.Fake().has_attr(connection=conn)
        count = evolve4.do_evolve(context)
        assert_that(count, is_(1))

        index = catalog[IX_MIMETYPE]
        assert_that(index.values_to_documents.get('foo/foo'),
                    contains(doc_id))