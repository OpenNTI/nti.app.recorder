#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
.. $Id$
"""

from __future__ import print_function, unicode_literals, absolute_import, division
__docformat__ = "restructuredtext en"

logger = __import__('logging').getLogger(__name__)

import time
from datetime import date
from datetime import datetime

from six import string_types

from zope.interface.common.idatetime import IDate
from zope.interface.common.idatetime import IDateTime


def is_valid_timestamp(ts):
    try:
        ts = float(ts)
        return ts >= 0
    except (TypeError, ValueError):
        return False


def parse_datetime(t):
    result = t
    if t is None:
        result = None
    elif is_valid_timestamp(t):
        result = float(t)
    elif isinstance(t, string_types):
        try:
            result = IDateTime(t)
        except Exception:
            result = IDate(t)
        result = time.mktime(result.timetuple())
    elif isinstance(t, (date, datetime)):
        result = time.mktime(t.timetuple())
    if not isinstance(result, float):
        raise ValueError("Invalid date[time]")
    return result
