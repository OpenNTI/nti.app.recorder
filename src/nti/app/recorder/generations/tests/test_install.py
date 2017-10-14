#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import division
from __future__ import print_function
from __future__ import absolute_import

# disable: accessing protected members, too many methods
# pylint: disable=W0212,R0904

from hamcrest import has_key
from hamcrest import assert_that

import unittest

from nti.app.recorder.tests import SharedConfiguringTestLayer

from nti.dataserver.tests import mock_dataserver


class TestFunctionalInstall(unittest.TestCase):

    layer = SharedConfiguringTestLayer

    @mock_dataserver.WithMockDSTrans
    def test_installed(self):
        conn = mock_dataserver.current_transaction
        root = conn.root()
        generations = root['zope.generations']
        assert_that(generations, has_key('nti.dataserver-recorder'))
