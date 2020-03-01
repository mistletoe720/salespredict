# -*- coding: utf-8 -*-
"""
封装 MySQL 2 HIVE 的数据备份，此备份针对的是有组织有计划的数据备份，如果是单次的数据表备份，直接使用pyhive中的函数即可.

将MySQL的数据备份到hive分区表，暂时是每天的销量预测结果，后面可以备份其他数据，方便后期回查数据，同时避免MySQL数据拥挤。

注意，要求hive表和MySQL表的字段一致，如果后来MySQL表的结构有变化，比如增减字段，则需要手动处理hive表结构。

注意，所有的备份表都应该是日期字段分区的表，如果没有日期字段，请手动增加日期字段后再进行备份。

为了应对程序出错，增加未备份的处理问题。

hive建分区表的方法：
drop table if exists tmp.tmp_szy_test;
create table tmp.tmp_szy_test (id int, name string) partitioned BY ( dd string )
ROW FORMAT DELIMITED FIELDS TERMINATED BY ',' ;

注意，一个分区中，分区字段的值应该都是一样的，这样才能归为一个分区，比如日期都是1月1日

获取MySQL建表语句的方法：
show create table tb_name

步骤：
1.检查哪些表需要备份
2.对比MySQL和hive中表的结构是否对应，如果不对应，报错
3.对表的每个需要备份的分区（日期），取出对应的分区数据，然后保存到csv，最后将csv写入hive分区



config：
{ 'source_tb': 'tb_name1',        MySQL源表
  'target_tb': 'tb_name2',        hive目标表
  'partition_col': ['statedate']  注意把日期字段放在第一个位置，如果有多个分区字段的话
  'frequence': 1,2,3,4            表示每1(2,3,4)天备份，1表示每天备份，2表示每2天备份1次，3表示每3天备份1次
  'job_hour': [5, 12, 17]         表示5点，12点，17点的时候备份，当一天多次备份时候有用，多天一次相当于指定什么时候启动任务备份
  'describe': 'describe'          任务描述
}

步骤：
1、备份任务每小时执行1次
2、逐个表循环
3、判断这个表是否已经备份（防止同一任务重复执行）
4、取出上次时间到现在的数据的所有分区
5、逐个分区取数，然后导入到hive

判断是否重复备份也很简单，
1、如果是多天才备份一次，首先看今天和上次备份时间的间隔天数是否够n天，其次是要求今天的时间点大于指定的时间点，比如现在是11点，但是备份要求12点才启动

2、如果一天备份1次或多次，也很好办，首先创建一个时间轴，在上面标注哪些时间点要备份的，然后判断上次时间是否在合理的时间区间内（理论上的上次备份时间和理论上的下次备份时间的区间），如果不在说明备份区间内，说明需要备份

3、备份是从上次备份时间点再往前一小段时间开始备份的

"""

import pandas as pd
import datetime
import os
# from aipurchase.common.database import pyhive
# from aipurchase.common.database import pymysql
# from aipurchase.common.database import pyfile
# from aipurchase.common.log import log as _log
from . import pyhive
from . import pymysql
from . import pyfile
from ..log import log as _log

pyos = pyfile.pyos()
pyjson = pyfile.pyjson()


def time_to_hour(time_string):
    """ 把时分秒换算成小时 """
    return pd.to_datetime(time_string).hour  # time_string='2015-12-15 12:12:12'


def time_sub(time1, time2):
    """统计两个时间差的分钟数"""
    # time1, time2 = '2015-12-13 15:12:12', '2015-12-12 13:12:12'
    return int((pd.to_datetime(time1) - pd.to_datetime(time2)).seconds / 60)


def day_sub(time1, time2):
    """两个时间差的天数，不考虑时分秒的影响"""
    return (pd.to_datetime(time1[:10]) - pd.to_datetime(time2[:10])).days


def backup_batch_in_theory(frequence, job_hour):
    """
    从理论上判断在当前时间点一个任务对应的备份批次
    比如一个任务是 7,10,17 三个时间点备份，现在是11点，对应的备份任务是 2018-10-16 10:00:00
    当然，多天备份一次的比较麻烦
    """
    # job_hour=[5, 12, 17]
    # frequence=1
    # 创建一年多时间长度的时间轴，轴上是每个需要备份的时间点
    start = str(datetime.datetime.now() - datetime.timedelta(frequence * 365))
    end = str(datetime.datetime.now() + datetime.timedelta(frequence * 10))
    date_cycle = pd.date_range(start=start, end=end, freq='1H')
    date_cycle = [d.strftime('%Y-%m-%d %H:00:00') for d in date_cycle if d.hour in job_hour]
    # 判断当前时间对应的备份批次，即上次时间和下次时间
    now = str(datetime.datetime.now())[:19]
    for i in range(1, len(date_cycle)):
        last_job = date_cycle[i - 1]
        next_job = date_cycle[i]
        if last_job <= now <= next_job:
            # print(last_job, next_job)
            return last_job, next_job


def do_not_repeat_backup(back_info_file, tb_name, frequency=None, job_hour=None, mode='read'):
    """
    对于重试等，如果某个表已经正常备份，在失败重试阶段就没必要再次备份了
    判断的标准是，如果上一次执行时间是在备份时间区间内，说明没必要重复备份
    back_info_file：备份时间信息的文件路径
    tb_name: 需要备份的MySQL表名
    backup_times：一天备份多少次
    """
    # 先读取上次备份的时间信息
    if not pyos.file_exists(back_info_file):
        last_backup = {}
    else:
        last_backup = pyjson.read(back_info_file)
    #
    now = str(datetime.datetime.now())[:19]
    # 判断是否需要备份，特别是失败重试的情况下需要做这样的处理
    if mode == 'read':
        last_time = last_backup.get(tb_name, '2017-01-01 01:01:01')
        # 如果是多天备份一次，看上次执行时间和当前时间是否超过fre天
        # 如果超过了fre天，且达到备份时间点，则说明需要备份
        #
        if frequency > 1 and day_sub(now, last_time) >= frequency and int(now[11:13]) > job_hour[0]:
            return False, last_time  # false表示需要备份
        #
        # 如果是1天1次或者1天多次，就比较简单了
        last_job, next_job = backup_batch_in_theory(frequency, job_hour)
        if frequency == 1 and last_job <= last_time <= next_job:
            print('表：%s 上次备份的时间是%s，当前时间是：%s，不需要重复备份' % (tb_name, last_time, now))
            return True, last_time  # 不需要备份
        else:
            return False, last_time  # 需要备份
    # 如果备份完成，更新json信息
    if mode == 'update':
        last_backup[tb_name] = now
        pyjson.write(back_info_file, last_backup)


def mysqltype_to_hivetype(mysqltype):
    """MySQL数据类型映射为hive的数据类型，当前只支持3中"""
    if 'bigint' in mysqltype:
        hivetype = 'bigint'
    if 'float' in mysqltype:
        hivetype = mysqltype.replace('float', 'decimal')
    if 'varchar' in mysqltype:
        hivetype = 'string'
    elif 'int' in mysqltype:
        hivetype = 'int'
    return hivetype


def get_mysql_schema(ai_mysql, tb_name):
    """获取MySQL表的字段名称和数据类型，MySQL类型转hive"""
    # tb_name = tb_name.split('.')[0].upper()+'.'+tb_name.split('.')[1]
    schema = ai_mysql.read_table(sql="desc " + tb_name)
    schema['type'] = schema['type'].apply(lambda x: mysqltype_to_hivetype(x))
    schema = dict([(col, dtype) for col, dtype in zip(schema['field'].tolist(), schema['type'].tolist())])
    return schema


def get_hive_schema(ai_hive, tb_name):
    """获取hive表的字段名称和数据类型"""
    schema = pd.read_sql("desc " + tb_name, ai_hive.conn)
    schema = schema.fillna('')
    for i in range(len(schema)):
        if schema.loc[i, 'col_name'] == '':
            break
    schema = schema.loc[: i - 1]
    # schema = schema.loc[schema['col_name']!=''].drop_duplicates(subset='col_name')
    schema = dict([(col, dtype) for col, dtype in zip(schema['col_name'].tolist(), schema['data_type'].tolist())])
    return schema


def create_hive_tb(ai_hive, tb_name2, mysql_schema, partition_col=[]):
    """如果hvie中不存在对应的表，则创建"""
    sql_col1 = []
    sql_col2 = []
    for col, dtype in mysql_schema.items():
        if col not in partition_col:
            sql_col1.append(" `%s` %s" % (col, dtype))
        else:
            sql_col2.append(" `%s` %s" % (col, dtype))
    sql_col1 = ', \n'.join(sql_col1)
    sql_col2 = ', \n'.join(sql_col2)
    sql1 = """CREATE TABLE `%s`(\n%s)\n""" % (tb_name2, sql_col1)
    sql2 = """PARTITIONED BY (\n%s)\n""" % sql_col2
    sql3 = """ROW FORMAT DELIMITED\n FIELDS TERMINATED BY '\001' """
    if partition_col:
        sql = sql1 + sql2 + sql3
    else:
        sql = sql1 + sql3
    print('创建hive表%s：\n%s' % (tb_name2, sql))
    ai_hive.execute(sql)
    print('建表成功！')
    # 获取字段名称和数据类型


def check_columns_between_mysql_and_hive(ai_mysql, ai_hive, tb_name1, tb_name2, partition_col=[]):
    """
    对比MySQL和hive库中表的字段差异.
    """
    # 获取MySQL的字段名称和数据类型
    mysql_cols = get_mysql_schema(ai_mysql, tb_name1)
    for col in partition_col:
        if col in mysql_cols.keys():
            mysql_cols[col] = 'string'
    # 获取hive的字段名称和数据类型
    try:
        hive_cols = get_hive_schema(ai_hive, tb_name2)
    except:
        create_hive_tb(ai_hive, tb_name2, mysql_cols, partition_col)
    finally:
        hive_cols = get_hive_schema(ai_hive, tb_name2)
    for col in partition_col:
        if col in hive_cols.keys():
            mysql_cols[col] = 'string'
    #
    # 找那些在MySQL表有而在hive表没有的字段
    exist_mysql_col = []
    for k1, v1 in mysql_cols.items():
        if k1 not in hive_cols.keys():
            exist_mysql_col.append((k1, v1))
    if len(exist_mysql_col):
        print('警告：字段不匹配，有如下字段是在MySQL表有而在hive表没有的字段：' + str(exist_mysql_col))
    #
    # 反过来，找那些在hive表有而mysql表没有的字段
    exist_hive_col = []
    for k2, v2 in hive_cols.items():
        if k2 not in mysql_cols.keys():
            exist_hive_col.append((k2, v2))
    if len(exist_hive_col):
        print('警告：字段不匹配，有如下字段是在hive表有而mysql表没有的字段：' + str(exist_hive_col))
    #
    return exist_mysql_col, exist_hive_col


def add_cols_that_not_exists_hive(ai_hive, tb_name, exist_mysql_col, partition_col=[], all_partitions=None):
    """对于某些在MySQL表中有，而在hive表中没有的字段（这里指的是非分区字段），需要在hive表里面新增"""
    tb_name_list = tb_name.split('.')
    database = tb_name_list[0]
    tb_name2 = tb_name_list[-1]
    for col, dtype in exist_mysql_col:
        cur = ai_hive.conn.cursor()
        cur.execute("use %s" % database)
        cur.execute("alter table %s add columns(%s %s)" % (tb_name, col, dtype))
        # 如果存在分区
        if partition_col:
            for i in range(len(all_partitions)):
                if len(partition_col) == 1:
                    sql = "alter table %s partition(%s = '%s') add columns(%s %s)" \
                          % (tb_name, partition_col[0], all_partitions.iloc[i, 0], col, dtype)
                if len(partition_col) == 2:
                    sql = "alter table %s partition(%s = '%s', %s = '%s') add columns(%s %s)" \
                          % (tb_name, partition_col[0], all_partitions.iloc[i, 0],
                             partition_col[1], all_partitions.iloc[i, 1], col, dtype)
                try:
                    # 分区在新增字段前存在
                    cur.execute(sql)
                except:
                    # 分区在新增字段前不存在
                    continue
        ai_hive.conn.commit()
        cur.close()
        print('在hive表%s新增字段(%s %s)成功' % (tb_name, col, dtype))


# def drop_cols_that_not_exists_mysql(ai_hive, tb_name, exist_hive_col):
#     """对于某些在MySQL表中没有，而在hive表中有的字段，需要从hive表里面删除"""
#     for col, dtype in exist_hive_col:
#         sql = 'alter table %s drop column %s' % (tb_name, col)
#         cur = ai_hive.conn.cursor()
#         cur.execute(sql)
#         ai_hive.conn.commit()
#         cur.close()

def add_cols_that_not_exists_mysql(df, exist_hive_col):
    """对于某些在hive表中有，而在MySQL表中没有的字段，需要补充，否则导入hive后会出现字段不匹配"""
    for col, dtype in exist_hive_col:
        if 'string' in str(dtype).lower() or 'object' in str(dtype).lower():
            df[col] = '-1'
        else:
            df[col] = -1
    return df


def back_tb_data(ai_mysql, ai_hive, tb_name1, tb_name2, last_time, partition_col, local_path, log):
    """根据数据表中的日期，以及上次备份时间，取出需要备份的数据"""
    # 第一步：看看有哪些分区数据还没有备份
    # 第二步：读取一个分区
    # 第三步：对比MySQL表和hive表的字段差异，增补字段
    # 第四步：数据导入hive
    last_time = str(pd.to_datetime(last_time))[:10]
    partition_field = ', '.join(partition_col)
    partitions_sql = "select distinct %s from %s where %s >= '%s' " % \
                     (partition_field, tb_name1, partition_col[0], last_time)
    all_partitions = ai_mysql.read_table(sql=partitions_sql).applymap(str)
    log.info('还需要备份的分区是：\n' + log.create_pretty_table(all_partitions))
    # 字段检查
    exist_mysql_col, exist_hive_col = check_columns_between_mysql_and_hive(
        ai_mysql, ai_hive, tb_name1, tb_name2, partition_col)
    # 在hive表里面新增字段
    if len(exist_mysql_col):
        add_cols_that_not_exists_hive(ai_hive, tb_name2, exist_mysql_col, partition_col, all_partitions)
    # 每个分区组合读取一次
    for i in range(len(all_partitions)):
        if len(partition_col) == 1:
            sql = "select * from %s where %s = '%s' "
            partition_value = (all_partitions.iloc[i, 0],)
            args = (tb_name1, partition_col[0], all_partitions.iloc[i, 0])
        else:
            sql = "select * from %s where %s = '%s' "
            partition_value = [all_partitions.iloc[i, 0]]
            args = [tb_name1, partition_col[0], all_partitions.iloc[i, 0]]
            for j in range(1, len(partition_col)):
                sql = sql + "and %s = '%s' "
                partition_value.append(all_partitions.iloc[i, j])
                args.extend([partition_col[j], all_partitions.iloc[i, j]])
            partition_value = tuple(partition_value)
            args = tuple(args)
        # 读取MySQL数据
        mysql_sql = sql % args
        df = ai_mysql.read_table(sql=mysql_sql)
        log.info('SQL：%s'%mysql_sql)
        log.info('读取 %s 表的 partition=%s 的数据，数据量：%d' % (tb_name1, str(partition_value), len(df)))
        if len(df)==0:
            log.info('没有数据，不需要转成到hive')
            continue
        # 用一个比较特别的数据填充hive有MySQL无的字段
        if len(exist_hive_col):
            df = add_cols_that_not_exists_mysql(df, exist_hive_col)
        # 保存到hive
        ok, error = ai_hive.load_df_into_partition_table2(tb_name=tb_name2,
                                                          df=df,
                                                          partition_col=partition_col,
                                                          partition_value=partition_value,
                                                          local_path=local_path,
                                                          sep='\001')
        if not ok:
            raise Exception(error)
        log.info('导入MySQL %s的数据到hive分区：partition=%s' % (tb_name1, str(partition_value)))
    log.info('完成 %s 的所有分区备份' % tb_name1)


class demo_config():
    """配置数据备份信息"""
    # 定义哪些MySQL数据表需要备份到hive
    # 需要注意的是，MySQL正式库，只允许查不允许写
    #
    # tb_name1：需要备份的MySQL数据表
    # tb_name2：备份到hive的表名
    # partition_col：分区字段，最多不超过两个
    # backup_times：一天备份的次数
    # frequency：频率，即备份多少天之前的数据，在多天才备份一次的场景下有用
    # sql：提取每个分区组合数据的SQL语句
    # load_sql：从HDFS覆盖导入数据到hive的代码
    # describe：数据表描述

    # 定义需要备份到hive的MySQL表
    backup_tb = [
        { 'source_tb': 'tb_name1',          # MySQL源表
            'target_tb': 'tb_name2',        # hive目标表
            'partition_col': ['statedate'], # 注意把日期字段放在第一个位置，如果有多个分区字段的话
            'frequency': 1,                 # 表示每1(2,3,4)天备份，1表示每天备份，2表示每2天备份1次，3表示每3天备份1次
            'job_hour': [5, 12, 17]         # 表示5点，12点，17点的时候备份，当一天多次备份时候有用，多天一次相当于指定什么时候启动任务备份
        },
    ]
    # 本地备份数据表相关信息
    project_path = '/home/dm/suzhenyu'
    filename = os.path.join(project_path, 'table_last_backup_time.json')
    # 临时数据文件路径
    local_path = r'/home/dm/data_tmp'


def demo():
    """其他程序或者模块需要备份数据，仿照这个demo实现即可"""
    #
    # 连接MySQL，hive数据库
    statedate = str(datetime.datetime.now())[:10]
    log = _log.logger(logname='mysql2hive_databack')
    #
    ai_mysql = pymysql.mysql(query='ai_mysql')
    ai_hive = pyhive.pyhive(query='ai_hive')
    log.info('连接MySQL，hive数据库')
    #
    # 从config中读取那些表需要备份
    # 为了方便写代码这里将config配置成一个class，实际使用时应该写成配置文件
    config = demo_config()
    back_info_file = config.filename
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
        repeat, last_time = do_not_repeat_backup(back_info_file = back_info_file, tb_name=tb_name1, frequency=frequency, job_hour=job_hour, mode='read')
        # 已经备份过的数据就不进行备份了
        if repeat:
            continue
        #
        # 备份其他未备份的数据
        # 获取上次备份时间
        last_time = str(pd.to_datetime(statedate) - datetime.timedelta(days=frequency))[:10]
        # 备份
        back_tb_data(ai_mysql, ai_hive, tb_name1, tb_name2, last_time, partition_col, local_path, log)
        # 更新备份表信息
        do_not_repeat_backup(back_info_file = back_info_file, tb_name = tb_name1, mode='update')
    #
    log.info('完成所有表的数据备份')
