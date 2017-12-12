#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# pylint: disable=protected-access,too-many-public-methods

from hamcrest import has_entry
from hamcrest import has_length
from hamcrest import assert_that

from zope import interface

from zope.annotation.interfaces import IAttributeAnnotatable

from nti.app.testing.application_webtest import ApplicationLayerTest

from nti.app.testing.decorators import WithSharedApplicationMockDS

from nti.base.interfaces import ICreated

from nti.dataserver.tests import mock_dataserver

from nti.dataserver.users.users import User

from nti.ntiids.oids import to_external_ntiid_oid

from nti.recorder.mixins import RecordableMixin

from nti.recorder.record import get_transactions

from nti.recorder.utils import record_transaction

from nti.zodb.persistentproperty import PersistentPropertyHolder


@interface.implementer(ICreated, IAttributeAnnotatable)
class Bleach(PersistentPropertyHolder, RecordableMixin):
    __name__ = None
    __parent__ = None


class TestViews(ApplicationLayerTest):

    def _create_ichigo(self):
        user = User.get_user(self.default_username)
        ichigo = Bleach()
        current_transaction = mock_dataserver.current_transaction
        current_transaction.add(ichigo)
        self.ds.root['ichigo'] = ichigo
        ichigo.creator = user  # pylint: disable=attribute-defined-outside-init
        record_transaction(ichigo,
                           principal=user,
                           type_=u"Activation",
                           ext_value={u'shikai': True},
                           createdTime=1000)
        record_transaction(ichigo,
                           principal=user,
                           type_=u"Activation",
                           ext_value={u'bankai': True},
                           createdTime=2000)
        return ichigo

    @WithSharedApplicationMockDS(users=True, testapp=True)
    def test_audit_logs(self):
        with mock_dataserver.mock_db_trans(self.ds):
            ichigo = self._create_ichigo()
            assert_that(get_transactions(ichigo),  has_length(2))

        url = '/dataserver2/users/%s/@@audit_log'
        res = self.testapp.get(url % self.default_username,
                               status=200)
        assert_that(res.json_body, has_entry('Items', has_length(2)))

    @WithSharedApplicationMockDS(users=True, testapp=True)
    def test_trim_logs(self):
        with mock_dataserver.mock_db_trans(self.ds):
            ichigo = self._create_ichigo()
            assert_that(get_transactions(ichigo),  has_length(2))
            rec_oid = to_external_ntiid_oid(ichigo)

        url = '/dataserver2/Objects/%s/@@trim_log'
        res = self.testapp.post_json(url % rec_oid,
                                     {'startTime': 1200},
                                     status=200)
        assert_that(res.json_body, has_entry('Items', has_length(1)))

        with mock_dataserver.mock_db_trans(self.ds):
            ichigo = self.ds.root['ichigo']
            transactions = get_transactions(ichigo)
            assert_that(transactions, has_length(1))

    @WithSharedApplicationMockDS(users=True, testapp=True)
    def test_clear_logs(self):
        with mock_dataserver.mock_db_trans(self.ds):
            ichigo = self._create_ichigo()
            rec_oid = to_external_ntiid_oid(ichigo)

        url = '/dataserver2/Objects/%s/@@audit_log'
        res = self.testapp.get(url % rec_oid,
                               status=200)
        assert_that(res.json_body, has_entry('Items', has_length(2)))

        url = '/dataserver2/Objects/%s/@@clear_log'
        res = self.testapp.post_json(url % rec_oid,
                                     status=200)
        assert_that(res.json_body, has_entry('Items', has_length(2)))

        with mock_dataserver.mock_db_trans(self.ds):
            ichigo = self.ds.root['ichigo']
            transactions = get_transactions(ichigo)
            assert_that(transactions, has_length(0))

    @WithSharedApplicationMockDS(users=True, testapp=True)
    def test_get_delete_log(self):
        with mock_dataserver.mock_db_trans(self.ds):
            ichigo = self._create_ichigo()
            transactions = get_transactions(ichigo)
            assert_that(transactions, has_length(2))
            oid = to_external_ntiid_oid(transactions[0])

        url = '/dataserver2/Objects/%s/'
        self.testapp.delete(url % oid, status=204)

        with mock_dataserver.mock_db_trans(self.ds):
            ichigo = self.ds.root['ichigo']
            assert_that(get_transactions(ichigo), has_length(1))
