# -*- coding: utf-8 -*-
"""
Python 连接 sqlserver 提取数据
要先安装 msodbcsql
"""
import pymssql as pymssql_
import pandas as pd
import base64
import traceback
import datetime
from ...config.config import mssql_paramsdbslave

class pymssql():
    def __init__(self,host=None, port=None,user=None, password=None,database='sellercube',charset="utf8",encrypt=True):
        self.host=host
        self.port=port
        self.user=user
        self.password=password
        self.database=database
        self.charset=charset
        self.encrypt=encrypt
        self.conn=None
        self._init_connect()

    def _init_connect(self):
        """连接数据库"""
        # 建立链接，创建查询句柄
        # charset="utf8"
        # 连接msserver很奇怪的，host加密解密看起来明明是同一个字符串，但就是连不上
        if not self.host:
            self.host = mssql_paramsdbslave[0]
            self.port = mssql_paramsdbslave[1]
            self.user = mssql_paramsdbslave[2]
            self.password = mssql_paramsdbslave[3]
        if self.encrypt:
            decode = lambda string: base64.b64decode(string).decode()
            host, port, user, passwd = decode(self.host), decode(self.port), decode(self.user), decode(self.password)
        else:
            host, port, user, passwd = self.host, self.port, self.user, self.password
        database = self.database
        conn = pymssql_.connect(host=host, port=port, user=user, password=passwd, database=database)
        conn.autocommit(True)
        self.conn=conn

    def get_conn(self):
        """检测如果断开则重新连接"""
        try:
            self.conn._conn.connected
        except:
            self._init_connect()
            print('检测到连接断开，重新连接成功')
        return self.conn

    def close(self):
        """关闭数据库连接"""
        self.conn.close()

    def read_table(self, tb_name=None, sql=None):
        """读取数据库数据"""
        if not sql:
            sql = "select * from {tb_name}".format(tb_name=tb_name)
        conn=self.get_conn()
        data = pd.read_sql(sql, conn)
        data.columns = [col.lower().split('.')[-1] for col in data.columns]
        return data

    def read_big_table(self, tb_name=None, sql=None, each_fetch_size=100000):
        """
        读取大表数据，在读取大表数据时，由于超时或者数据量过大导致连接时效的问题。
        针对这个问题，可以分批次读取，以及设置最大传输量等。
        """
        # info = """read_big_table 函数用于读取MySQL大表数据，因为mysql可能会出现数据量过大而超时，溢出等异常，因此需要特殊处理。
        # 方法有：conn.max_allowed_packet=67108864. 不是一次读取全部数据，而是分批次一次读取10w ...等 """
        # print('-' * 80)
        # print(info)
        # 获取SQL
        if not sql:
            sql = "select * from {tb_name}".format(tb_name=tb_name)
        # 获取数据库连接
        conn=self.get_conn()
        cur=conn.cursor()
        # 第一步：获取表头字段名
        cols_sql = sql + ' limit 1'
        cols = pd.read_sql(cols_sql, conn).columns.tolist()
        cols = [col.lower().replace(' ', '').split('.')[-1] for col in cols]
        # print('获取的字段名是：' + ','.join(cols))
        # 分批次读取
        try:
            data = []
            cur.execute(sql)
            i = 0
            while True:
                sub_data = [row for row in cur.fetchmany(each_fetch_size)]  # 每次读取10w条记录
                if len(sub_data) > 0:
                    data.extend(sub_data)
                    t = str(datetime.datetime.now())[:19]
                    print('%s 读取数据 [%d-%d)' % (t, i, (i + len(sub_data))))
                    i += len(sub_data)
                else:
                    break
        except:
            error = traceback.format_exc()
            # 返回错误之前，记得先关闭连接
            cur.close()
            conn.close()
            raise Exception(error)
        # 如果正常，记得关闭cursor
        cur.close()
        # 转成dataframe
        data2 = pd.DataFrame(data, columns=cols)
        # print('-' * 80)
        return data2


def test():
    # 建立连接
    ms = pymssql()
    # 查询语句
    sql="""select top 10 * from sellercube..product"""
    ms.read_table(sql=sql)
    # 关闭连接
    ms.close()
