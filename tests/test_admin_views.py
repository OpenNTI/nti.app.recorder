#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import is_
from hamcrest import has_entry
from hamcrest import has_length
from hamcrest import assert_that

from nti.externalization.interfaces import StandardExternalFields

from nti.app.testing.application_webtest import ApplicationLayerTest

from nti.app.testing.decorators import WithSharedApplicationMockDS

INTID = StandardExternalFields.INTID
NTIID = StandardExternalFields.NTIID

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
		res = self.testapp.get(path,  params={'startTime': 0}, status=200)
		assert_that(res.json_body, has_entry('ItemCount', is_(0)))
		assert_that(res.json_body, has_entry('Items', has_length(0)))
