#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Xiang Wang <ramwin@qq.com>

# pylint: disable=C


import datetime
import unittest

from redis import Redis

from duration2.tasks import RedisDurationTask


task = RedisDurationTask(Redis(decode_responses=True))


class Test(unittest.TestCase):

    def setUp(self):
        task.clear()

    def test(self):
        datetime1 = datetime.datetime(
            2024, 1, 1, 13, 0, 0)
        datetime2 = datetime.datetime(
            2024, 1, 1, 14, 0, 0)
        datetime3 = datetime.datetime(
            2024, 1, 1, 13, 59, 0)
        task.create_task("task1", datetime1)
        task.create_task("task2", datetime2)
        task.create_task("task1", datetime3)
        tasks = task.get_tasks()
        self.assertEqual(
            tasks,
            [
                "task1_28401420",
            ]
        )
