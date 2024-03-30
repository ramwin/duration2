#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Xiang Wang <ramwin@qq.com>


from unittest import TestCase

from duration2.datatype import RedisLimitedTimeList
from redis import Redis


class Test(TestCase):

    def setUp(self):
        self.task = RedisLimitedTimeList("students_grades:1", max_count=3)

    def tearDown(self):
        self.task.clear()

    def test_redis_limited_timelist(self):
        self.task.add_data
