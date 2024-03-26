#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Xiang Wang <ramwin@qq.com>


# pylint: disable=missing-function-docstring


"""
use redis to dedupe task
"""


import datetime
import time
import uuid

from typing import List, Tuple, Union, overload, Literal, Optional

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

    def create_task(self, task_id: str, date_time: datetime.datetime) -> int:
        """
        where the task was new created
            0: not created
            1: created
        """
        index = self.interval.get_index(date_time)
        return self.client.zadd(
            self.key,
            {
                f"{task_id}_{index}": index
            }
        )

    @overload
    def get_tasks(self, count: int, parse: Literal[False], timeout: int) -> List[str]:
        ...
    @overload
    def get_tasks(self, count: int, parse: Literal[True], timeout: int) -> List[Tuple[str, Interval]]:
        ...
    def get_tasks(self, count: int = 10, parse=False, timeout=0) -> Union[List[str], List[Tuple[str, Interval]]]:
        tasks = self.client.zpopmin(self.key, count=count)
        if not tasks and timeout:
            task = self.client.bzpopmin(self.key, timeout=timeout)
            if task:
                tasks = [task]
        task_id_index_list = [
            i[0]
            for i in tasks
        ]
        if not parse:
            return task_id_index_list
        return [
            self.parse_task(task_id_index)
            for task_id_index in task_id_index_list
        ]

    def parse_task(self, task_index: Union[str, bytes]) -> Tuple[str, Interval]:
        if isinstance(task_index, bytes):
            task_index = task_index.decode("UTF-8")
        task_id, index = task_index.rsplit("_", 1)
        index_int = int(index)
        return task_id, self.interval.get_portion_from_index(index_int)

    @overload
    def get_pre_tasks(self, count: int,
                      date_time: Optional[datetime.datetime],
                      parse: Literal[False]) -> List[str]:
        ...
    @overload
    def get_pre_tasks(self, count: int,
                      date_time: Optional[datetime.datetime],
                      parse: Literal[True]) -> List[Tuple[str, Interval]]:
        ...
    def get_pre_tasks(self, count: int = 10,
                      date_time: Optional[datetime.datetime] = None,
                      parse: bool = False,
                      ) -> Union[List[str], List[Tuple[str, Interval]]]:
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
        return [
            self.parse_task(task_id_index)
            for task_id_index in handle_results
        ]

    def clear(self):
        self.client.delete(
            self.key,
        )


class ThresholdTask:
    """
    a task that will limit you speed
    timeout: The task will not run until after timeout seconds

    >>> a = ThresholdTask(timeout=1)
    >>> a.run()
    True
    >>> a.run()
    False
    >>> time.sleep(1)
    >>> a.run()
    True
    """

    def __init__(self, timeout: float):
        self.next_run: float = 0
        self.timeout = timeout

    def run(self) -> bool:
        if time.time() > self.next_run:
            self.next_run = time.time() + self.timeout
            return True
        return False


class RedisThresholdTask:
    """
    a task can limit the speed of a task. It will use a redis sorted set to 
    remember the history, and it support multi process share the same threshold

    threshold = RedisDurationTask(
        client=redis,
        max_cnt=6,
        interval=Interval(minutes=60),
    )
    threshold.run() will return True if it was called less then 6 times in the last minutes
    """

    def __init__(self, client: Redis,
                 max_cnt: int, interval: Union[float, datetime.timedelta],
                 task_name="",
                 ):
        self.client = client
        self.max_cnt = max_cnt
        if isinstance(interval, datetime.timedelta):
            self.interval = interval.total_seconds()
        else:
            self.interval = interval
        self.redis_key = f"redis_threshold_{task_name or uuid.uuid4().hex}"

    def run(self) -> bool:
        min_value = time.time() - self.interval
        self.client.zremrangebyscore(self.redis_key, "-inf", min_value)
        if self.client.zcard(self.redis_key) >= self.max_cnt:
            return False
        self.client.zadd(self.redis_key, {uuid.uuid4().hex: time.time()})
        return True
