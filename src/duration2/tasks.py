#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Xiang Wang <ramwin@qq.com>


# pylint: disable=missing-function-docstring


"""
use redis to dedupe task
"""


import datetime
from typing import List, Tuple, Union

from redis import Redis
from portion import Interval
from .base import TimeDelta


class RedisDurationTask:
    """
    a redis duration task can create task with a taskid
    """
    INTERVAL = TimeDelta(hours=1)
    KEY_PREFIX = "DURATION_TASK_"

    def __init__(self, client: Redis, interval=None, key_prefix=None):
        self.client = client
        self.interval = interval or self.INTERVAL
        if (self.interval.total_seconds() % 1) != 0:
            raise NotImplementedError("current not suppert micro seconds")
        if key_prefix is None:
            key_prefix = self.KEY_PREFIX
        self.key = key_prefix + str(int(self.interval.total_seconds()))

    def create_task(self, task_id: str, date_time: datetime.datetime):
        index = self.interval.get_index(date_time)
        self.client.zadd(
            self.key,
            {
                f"{task_id}_{index}": index
            }
        )

    def get_tasks(self, count: int = 10, parse=False) -> List[str]:
        tasks = self.client.zpopmin(self.key, count=count)
        task_id_index_list = [
            i[0]
            for i in tasks
        ]
        if not parse:
            return task_id_index_list
        else:
            return [
                self.parse_task(task_id_index)
                for task_id_index in task_id_index_list
            ]

    def parse_task(self, task_index: Union[str, bytes]) -> Tuple[str, Interval]:
        if isinstance(task_index, bytes):
            task_index = task_index.decode("UTF-8")
        task_id, index = task_index.rsplit("_", 1)
        index = int(index)
        return task_id, self.interval.get_portion_from_index(index)

    def get_pre_tasks(self, count: int = 10,
                      date_time: datetime.datetime = None,
                      parse=False,
                      ) -> List[str]:
        """
        only old tasks will be obtained.
        for example, a RedisDurationTask with TimeDelta(hours=1) will only get yesterday's task
        """
        tasks = self.client.zpopmin(self.key, count=count)
        if date_time is None:
            date_time = datetime.datetime.now()
        current_index = self.interval.get_index(date_time)
        handle_results = []
        un_handle_results = {}
        for task, score in tasks:
            if score >= current_index:
                un_handle_results[task] = score
            else:
                handle_results.append(task)
        if un_handle_results:
            self.client.zadd(self.key, un_handle_results)
        if not parse:
            return handle_results
        else:
            return [
                self.parse_task(task_id_index)
                for task_id_index in handle_results
            ]

    def clear(self):
        self.client.delete(
            self.key,
        )
