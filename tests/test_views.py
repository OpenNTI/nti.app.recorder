#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import has_entry
from hamcrest import has_length
from hamcrest import assert_that

from zope import interface

from zope.annotation.interfaces import IAttributeAnnotatable

from nti.base.interfaces import ICreated

from nti.coremetadata.interfaces import IRecordable

from nti.coremetadata.mixins import RecordableMixin

from nti.dataserver.users import User

from nti.externalization.oids import to_external_ntiid_oid

from nti.recorder.record import get_transactions

from nti.recorder.utils import record_transaction

from nti.zodb.persistentproperty import PersistentPropertyHolder

from nti.app.testing.application_webtest import ApplicationLayerTest

from nti.app.testing.decorators import WithSharedApplicationMockDS

from nti.dataserver.tests import mock_dataserver


@interface.implementer(IRecordable, ICreated, IAttributeAnnotatable)
class Bleach(PersistentPropertyHolder, RecordableMixin):
    __name__ = None
    __parent__ = None


class TestViews(ApplicationLayerTest):

    @WithSharedApplicationMockDS(users=True, testapp=True)
    def test_trim_logs(self):
        with mock_dataserver.mock_db_trans(self.ds):
            user = User.get_user(self.default_username)
            ichigo = Bleach()
            current_transaction = mock_dataserver.current_transaction
            current_transaction.add(ichigo)
            self.ds.root['ichigo'] = ichigo
            ichigo.creator = user
            record_transaction(ichigo,
                               principal=user,
                               type_="Activation",
                               ext_value={'shikai': True},
                               createdTime=1000)
            record_transaction(ichigo,
                               principal=user,
                               type_="Activation",
                               ext_value={'bankai': True},
                               createdTime=2000)
            assert_that(get_transactions(ichigo),  has_length(2))
            rec_oid = to_external_ntiid_oid(ichigo)

        res = self.testapp.post_json(
            '/dataserver2/Objects/%s/@@trim_log' % rec_oid,
            {'startTime': 1200},
            status=200)
        assert_that(res.json_body, has_entry('Items', has_length(1)))

        with mock_dataserver.mock_db_trans(self.ds):
            ichigo = self.ds.root['ichigo']
            transactions = get_transactions(ichigo)
            assert_that(transactions, has_length(1))
            oid = to_external_ntiid_oid(transactions[0])

        res = self.testapp.delete(
            '/dataserver2/Objects/%s/' % oid,
            status=204)

        with mock_dataserver.mock_db_trans(self.ds):
            ichigo = self.ds.root['ichigo']
            assert_that(get_transactions(ichigo), has_length(0))
