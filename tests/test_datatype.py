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
        self.task.add_data("second day", 2)
        self.task.add_data("third day", 3)
        self.task.add_data("first day", 1)
        self.assertEqual(
                self.task.get_last_data(),
                ("third day", 3),
        )
        self.task.add_data("forth day", -1)
        self.assertEqual(
                self.task.client.zcard(self.task.key), 3
        )
