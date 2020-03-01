# -*- coding: utf-8 -*-
"""
将mysql的数据保存到hive分区表
"""

import datetime
import pandas as pd
import datetime
import os
from .common.database import pyhive
from .common.database import pymysql
from .common.database import pyfile
from .common.log import log as _log
from .common.database import mysql2hive as m2h
from .business.data_backup import config

pyos = pyfile.pyos()
pyjson = pyfile.pyjson()


def main():
    """其他程序或者模块需要备份数据，仿照这个demo实现即可"""
    #
    # 连接MySQL，hive数据库
    statedate = str(datetime.datetime.now())[:10]
    log = _log.logger(logname='mysql2hive_databack')
    #
    ai_mysql = pymysql.mysql(query='')
    ai_hive = pyhive.pyhive(query='')
    log.info('连接MySQL，hive数据库')
    #
    # 从config中读取那些表需要备份
    # 为了方便写代码这里将config配置成一个class，实际使用时应该写成配置文件
    back_info_file = config.last_backup_info_file
    local_path = config.local_path
    #
    for table in config.backup_tb:
        # 读取需要备份的表信息
        tb_name1 = table['source_tb']
        tb_name2 = table['target_tb']
        partition_col = table['partition_col']
        frequency = table['frequency']
        job_hour = table['job_hour']
        # 首先判断是否会造成重复备份数据
        repeat, last_time = m2h.do_not_repeat_backup(back_info_file = back_info_file, tb_name=tb_name1, frequency=frequency, job_hour=job_hour, mode='read')
        # 已经备份过的数据就不进行备份了
        if repeat:
            continue
        #
        # 备份其他未备份的数据
        # 获取上次备份时间
        if last_time >= '2018-01-01':
            last_time = str(pd.to_datetime(statedate) - datetime.timedelta(days=frequency))[:10]
        else:
            last_time = str(pd.to_datetime(last_time) - datetime.timedelta(days=0))[:10]
        # 备份
        m2h.back_tb_data(ai_mysql, ai_hive, tb_name1, tb_name2, last_time, partition_col, local_path, log)
        # 更新备份表信息
        m2h.do_not_repeat_backup(back_info_file = back_info_file, tb_name = tb_name1, mode='update')
    #
    log.info('完成所有表的数据备份')


if __name__ == '__main__':
    main()
