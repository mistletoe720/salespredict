# -*- coding: utf-8 -*-
"""
python与sqlite数据库的交互。
python自带sqlite模块，可以创建sqlite数据库.
数据库入库,只要将全部数据转成字符串格式，sqlite会自动转成int，float，date等格式，和mysql差不多。


# 创建参数状态表

conn = sqlite3.connect(db_file)

create_sql=" create table params_server (pkey string, pvalue string) "
conn = sqlite3.connect(db_file)
conn.execute(create_sql)
conn.commit()

# 查询
sql = "select pvalue from params_server where pkey='abc' "
pd.read_sql(sql, conn)

"""

import pandas as pd
import sqlite3


class pysqlite():
    """用于作业状态存储，方便不同作业之间沟通信息，还是挺不错的"""

    def __init__(self, db_file):
        self._db = db_file
        # self.conn = self.connect()

    def connect(self):
        """连接数据库"""
        return sqlite3.connect(self._db)

    def execute(self, sql):
        """执行SQL"""
        conn = self.connect()
        conn.execute(sql)
        conn.commit()
        conn.close()

    def create_params_table(self, tb_name='params_server'):
        """创建参数状态表"""
        conn = self.connect()
        try:
            pd.read_sql('select * from %s' % tb_name, conn)
            conn.close()
            return
        except:
            print('参数状态表不存在，将创建：%s' % tb_name)
        # 建表
        create_sql = " create table params_server (pkey string, pvalue string) "
        self.execute(create_sql)

    def get_param(self, pkey='', pvalue=None, tb_name='params_server'):
        """模仿pymysql的状态表信息获取"""
        pkey = str(pkey).lower().strip()
        conn = self.connect()
        sql = """select pvalue from %s where pkey='%s' """ % (tb_name, pkey)
        df = pd.read_sql(sql, conn)
        conn.close()
        if len(df) == 0:
            print('指定的参数 %s 不存在' % pkey)
            return None
        else:
            return df['pvalue'].iat[0]

    def update_param(self, pkey='', pvalue='', tb_name='params_server'):
        """将指定参数更新到状态表"""
        pkey = str(pkey).lower().strip()
        pvalue = str(pvalue).lower().strip()
        # 先判断参数是否存在
        params = self.get_param(pkey)
        if params:
            sql = "update %s set %s = '%s' " % (tb_name, pkey, pvalue)
        else:
            sql = "insert into %s (pkey, pvalue) values ('%s', '%s')" % (tb_name, pkey, pvalue)
        print(sql)
        self.execute(sql)

    def clear_param(self, pkey='', tb_name='params_server'):
        """删除某个键"""
        pkey = str(pkey).lower().strip()
        sql = "delete from %s where pkey='%s'" % (tb_name, pkey)
        self.execute(sql)



def _test():
    db_file = r'E:\mytest\configobj\test_config.db'
    ai_params = pysqlite(db_file)

    # 如果参数状态表不存在就创建
    ai_params.create_params_table()

    # 查询参数是否存在
    ai_params.get_param('abc')

    # 更新参数
    ai_params.update_param(pkey='abc', pvalue='123')

    # 删除某个键
    ai_params.clear_param(pkey='abc')


def quick_insert():
    """sqlite3快速导入，暂时用不到"""
    # 打开数据库
    import time
    conn = sqlite3.connect(":memory:")
    conn.execute('PRAGMA synchronous = OFF')
    t1 = time.clock()
    sql='insert into table1 values (?,?,?)'
    df='Dataframe'
    conn.executemany(sql, df.to_records(index=False).tolist())
    t2 = time.clock()
    print('导入耗时：', t2 - t1)  # 17.9秒(100w*100)