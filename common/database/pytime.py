# -*- coding: utf-8 -*-
"""
时间处理相关模块
"""
import datetime
import time
from pandas import to_datetime


class pytime():
    def __init__(self):
        self._date_format = '%Y-%m-%d'
        self._time_format = '%Y-%m-%d %H:%M:%S'

    def today(self, format='%Y-%m-%d'):
        return datetime.datetime.today().strftime(format)

    def now(self, format='%Y-%m-%d %H:%M:%S'):
        return datetime.datetime.today().strftime(format)

    def day_add(self, date, add, format='%Y-%m-%d'):
        """日期加减"""
        return (to_datetime(date) + datetime.timedelta(add)).strftime(format)

    def time_add(self, time_, add_days=0, add_hour=0, add_mins=0, add_seconds=0,
                 format='%Y-%m-%d %H:%M:%S'):
        """时间加减"""
        # time_='12:12:12'
        seconds = add_days * 24 * 3600 + add_hour * 3600 + add_mins * 60 + add_seconds
        return (to_datetime(time_) + datetime.timedelta(seconds=seconds)).strftime(format)

    def last_month_last_day(self, statedate):
        """取上个月的最后一天"""
        # 原理很简单，就是取这个月的第一天，然后减去1秒就是上个月的最后一天了
        tt = to_datetime(to_datetime(statedate).strftime('%Y-%m-01')) - datetime.timedelta(seconds=1)
        return str(tt)[:10]

    def last_month_first_day(self, statedate):
        """取上个月的第一天"""
        tt = to_datetime(to_datetime(statedate).strftime('%Y-%m-01')) - datetime.timedelta(seconds=1)
        return tt.strftime('%Y-%m-01')

    def date_diff(self, time1, time2, diff='day'):
        """两个日期之间相差多少天或者多少秒"""
        # time1 = '2018-05-12'
        # time2 = '2019-12-12'
        delta = to_datetime(time1) - to_datetime(time2)
        # 返回月，默认一个月都是30天
        if diff == 'month':
            return delta.days / 30  # 带小数的
        # 返回周
        if diff == 'week':
            return delta.days / 7  # 带小数的
        # 返回天
        if diff == 'day':
            return delta.days
        # 返回小时
        if diff == 'hour':
            return delta.total_seconds() / 3600  # 带小数的
        # 返回分钟
        if diff == 'min':
            return delta.total_seconds() / 60  # 带小数的
        # 返回秒
        if diff == 'second':
            return delta.total_seconds()  # 带小数的
    