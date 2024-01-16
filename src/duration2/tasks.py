#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Xiang Wang <ramwin@qq.com>


# pylint: disable=missing-function-docstring


"""
use redis to dedupe task
"""


import datetime
from typing import List

from redis import Redis
from .base import TimeDelta


class RedisDurationTask:
    """
    a redis duration task can create task with a taskid
    """
    INTERVAL = TimeDelta(hours=1)
    KEY_PREFIX = "DURATION_TASK_"

    def __init__(self, client: Redis):
        self.client = client
        self.key = self.KEY_PREFIX + str(int(self.INTERVAL.total_seconds()))

    def create_task(self, task_id: str, date_time: datetime.datetime):
        index = self.INTERVAL.get_index(date_time)
        self.client.zadd(
            self.key,
            {
                f"{task_id}_{index}": index
            }
        )

    def get_tasks(self, count: int = 10) -> List[str]:
        tasks = self.client.zpopmin(self.key, count=count)
        return [
            i[0]
            for i in tasks
        ]

    def get_pre_tasks(self, count: int = 10,
                      date_time: datetime.datetime = None) -> List[str]:
        """
        only old tasks will be obtained.
        for example, a RedisDurationTask with TimeDelta(hours=1) will only get yesterday's task
        """
        tasks = self.client.zpopmin(self.key, count=count)
        if date_time is None:
            date_time = datetime.datetime.now()
        current_index = self.INTERVAL.get_index(date_time)
        handle_results = []
        un_handle_results = {}
        for task, score in tasks:
            if score >= current_index:
                un_handle_results[task] = score
            else:
                handle_results.append(task)
        self.client.zadd(self.key, un_handle_results)
        return handle_results

    def clear(self):
        self.client.delete(
            self.key,
        )
