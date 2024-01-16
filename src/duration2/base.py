#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Xiang Wang <ramwin@qq.com>

# pylint: disable=missing-class-docstring,missing-function-docstring


"""
basic class
"""


import datetime
from portion import closedopen


class TimeDelta(datetime.timedelta):

    def get_index(self, date_time: datetime.datetime):
        index, _ = divmod(date_time.timestamp(), self.total_seconds())
        return int(index)

    def get_portion(self, date_time):
        _, remain = divmod(date_time.timestamp(), self.total_seconds())
        return closedopen(
            date_time-datetime.timedelta(seconds=remain),
            date_time-datetime.timedelta(seconds=remain)+self
        )
