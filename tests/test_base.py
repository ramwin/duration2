#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Xiang Wang <ramwin@qq.com>


import datetime
import unittest
from duration2.base import TimeDelta


OneHour = TimeDelta(hours=1)


class TestBase(unittest.TestCase):

    def test_base(self):
        begin = datetime.datetime(
            1970, 1, 1,
            tzinfo=datetime.timezone.utc)
        self.assertEqual(
            OneHour.get_portion(begin).lower,
            begin
        )
