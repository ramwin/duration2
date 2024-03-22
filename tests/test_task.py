#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Xiang Wang <ramwin@qq.com>

# pylint: disable=C


import datetime
import logging
import time
import unittest

from threading import Thread

from redis import Redis

from duration2.tasks import RedisDurationTask, ThresholdTask, RedisThresholdTask


logging.basicConfig(level=logging.INFO)
task = RedisDurationTask(Redis(decode_responses=True))
LOGGER = logging.getLogger(__name__)


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

    def test_get_wait(self):
        task.clear()
        def f1():
            tasks = task.get_tasks(count=5, parse=True, timeout=1)
            self.assertTrue(tasks)
            LOGGER.info("task received: %s", tasks)
            tasks = task.get_tasks(count=5, parse=True, timeout=1)
            self.assertEqual(tasks, [])

        def f2():
            time.sleep(0.3)
            task.create_task("task1", datetime.datetime.now())
            LOGGER.info("task created: task1")

        task1 = Thread(group=None, target=f1)
        task2 = Thread(group=None, target=f2)
        task1.start()
        task2.start()
        start = time.time()
        self.assertFalse(
            task.get_tasks(count=5, parse=True, timeout=1)
        )
        end = time.time()
        self.assertGreater(end-start, 1)

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


class ThresHoldTest(unittest.TestCase):

    def test_thresholdtask(self):
        thresholdtask = ThresholdTask(timeout=0.1)
        self.assertTrue(thresholdtask.run())
        self.assertFalse(thresholdtask.run())
        time.sleep(0.01)
        self.assertFalse(thresholdtask.run())
        time.sleep(0.09)
        self.assertTrue(thresholdtask.run())

    def test_redis_thresholdtask(self):
        thresholdtask = RedisThresholdTask(client=Redis(), max_cnt=3, interval=1)
        for _ in range(3):
            self.assertTrue(thresholdtask.run())
            time.sleep(0.1)
        for _ in range(6):
            time.sleep(0.1)
            self.assertFalse(thresholdtask.run())
        time.sleep(0.15)
        self.assertTrue(thresholdtask.run())
        self.assertFalse(thresholdtask.run())
