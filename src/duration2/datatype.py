#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Xiang Wang <ramwin@qq.com>


from redis import Redis


class RedisLimitedTimeList:
    """
    a time base list(the time must be unique), it will auto shrink when the member is too much
    
    params:
        client: redis_client
        key: unique key stored in the redis
        max_count: if the member in the list exceed the max_count, it will pop the earliest data

    """

    def __init__(self, key: str, max_count: int, client: Redis=None):
        self.key = key
        self.max_count = max_count
        self.client = client or Redis()

    def add_data(self, data: str, timestamp: float) -> None:
        created = self.client.zadd(self.key, {data: timestamp})
        if created:
            self.shrink()

    def shrink(self) -> None:
        current_count = self.client.zcount(self.key)
        if current_count > self.max_count:
            self.client.zremrangebyrank(self.key, 0, self.max_count - current_count - 1)

    def clear(self):
        self.client.delete(self.key)
