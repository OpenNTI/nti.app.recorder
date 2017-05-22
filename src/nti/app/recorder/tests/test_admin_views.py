#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, absolute_import, division
__docformat__ = "restructuredtext en"

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import is_
from hamcrest import none
from hamcrest import is_in
from hamcrest import is_not
from hamcrest import has_entry
from hamcrest import has_length
from hamcrest import assert_that
from hamcrest import has_entries
from hamcrest import greater_than_or_equal_to

from zope import component
from zope import interface

from zope.annotation.interfaces import IAttributeAnnotatable

from zope.dublincore.interfaces import IDCExtended

from zope.intid.interfaces import IIntIds

from nti.dataserver.users import User

from nti.recorder.index import get_transactions

from nti.recorder.interfaces import IRecordable

from nti.recorder.mixins import RecordableMixin

from nti.recorder.utils import record_transaction

from nti.zodb.persistentproperty import PersistentPropertyHolder

from nti.app.testing.application_webtest import ApplicationLayerTest

from nti.app.testing.decorators import WithSharedApplicationMockDS

from nti.dataserver.tests import mock_dataserver


@interface.implementer(IRecordable, IAttributeAnnotatable)
class Bleach(PersistentPropertyHolder, RecordableMixin):
    pass


class TestAdminViews(ApplicationLayerTest):

    @WithSharedApplicationMockDS(users=True, testapp=True)
    def test_get_locked_objects(self):
        path = '/dataserver2/@@GetLockedObjects'
        res = self.testapp.get(path, status=200)
        assert_that(res.json_body, has_entry('Total', is_(0)))
        assert_that(res.json_body, has_entry('Items', has_length(0)))

    @WithSharedApplicationMockDS(users=True, testapp=True)
    def test_remove_all_trx_history(self):
        path = '/dataserver2/@@RemoveAllTransactionHistory'
        res = self.testapp.post(path, status=200)
        assert_that(res.json_body, has_entry('Recordables', is_(0)))
        assert_that(res.json_body, has_entry('RecordsRemoved', is_(0)))

    @WithSharedApplicationMockDS(users=True, testapp=True)
    def test_get_user_transaction_history(self):
        path = '/dataserver2/@@UserTransactionHistory'
        res = self.testapp.get(path, params={'startTime': 0}, status=200)
        assert_that(res.json_body, has_entry('ItemCount', is_(0)))
        assert_that(res.json_body, has_entry('Items', has_length(0)))

    @WithSharedApplicationMockDS(users=True, testapp=True)
    def test_trim_logs(self):
        with mock_dataserver.mock_db_trans(self.ds):
            user = User.get_user(self.default_username)
            ichigo = Bleach()
            current_transaction = mock_dataserver.current_transaction
            current_transaction.add(ichigo)
            self.ds.root['ichigo'] = ichigo
            ichigo.creator = user
            record = record_transaction(ichigo,
                                        principal=user,
                                        type_=u"Activation",
                                        ext_value={u'bankai': True})
            assert_that(record, is_not(none()))

            intids = component.getUtility(IIntIds)
            assert_that(intids.queryId(record), is_not(none()))

            transactions = list(get_transactions())
            assert_that(transactions, has_length(1))
            assert_that(record, is_in(transactions))

            adapted = IDCExtended(ichigo)
            assert_that(adapted.creators, is_(()))

    @WithSharedApplicationMockDS(users=True, testapp=True)
    def test_rebuild_catalogs(self):
        res = self.testapp.post('/dataserver2/@@RebuildTransactionCatalog',
                                status=200)
        assert_that(res.json_body,
                    has_entries('Total', is_(greater_than_or_equal_to(0)),
                                'ItemCount', is_(greater_than_or_equal_to(0))))

        res = self.testapp.post('/dataserver2/@@RebuildRecorderCatalog',
                                status=200)
        assert_that(res.json_body,
                    has_entries('Total', is_(greater_than_or_equal_to(0)),
                                'ItemCount', is_(greater_than_or_equal_to(0))))
