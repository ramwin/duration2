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
        datetime1 = datetime.datetime(
            2024, 1, 1, 13, 0, 0)
        datetime2 = datetime.datetime(
            2024, 1, 1, 14, 0, 0)
        datetime3 = datetime.datetime(
            2024, 1, 1, 13, 59, 0)
        task.create_task("task1", datetime1)
        task.create_task("task2", datetime2)
        task.create_task("task1", datetime3)
        task.create_task("task2", datetime3)

    def test(self):
        tasks = task.get_tasks()
        self.assertEqual(
            tasks,
            [
                "task1_473357",
                "task2_473357",
                "task2_473358",
            ]
        )
        info = task.parse_task("task1_473357")
        self.assertEqual(
                [info[0], info[1].lower],
                ["task1", datetime.datetime(2024, 1, 1, 13, 0, 0)],
        )

    def test_get_pre_tasks(self):
        self.assertFalse(
            RedisDurationTask(
                    Redis(decode_responses=True),
                    key_prefix="non"
            ).get_pre_tasks()
        )
        tasks = RedisDurationTask(
            Redis(decode_responses=True),
            key_prefix="DURATION_TASK_"
        ).get_pre_tasks(
            date_time=datetime.datetime(
                2024, 1, 1, 14, 59, 0),
            parse=True,
        )
        self.assertEqual(
            tasks[0][0],
            "task1",
        )
        self.assertEqual(
            tasks[1][1].lower,
            datetime.datetime(2024, 1, 1, 13, 0, 0),
        )
        tasks = task.get_tasks()
        self.assertEqual(
            tasks,
            [
                "task2_473358",
            ]
        )

    def test_get_pre_tasks2(self):
        task.clear()
        now = datetime.datetime.now()
        pre = now - task.INTERVAL
        task.create_task("task1", date_time=pre)
        task.create_task("task1", date_time=now)
        tasks = task.get_pre_tasks()
        self.assertEqual(len(tasks), 1)
        tasks = task.get_tasks()
        self.assertEqual(len(tasks), 1)
