# -*- coding: utf-8 -*-
"""
将hive的数据保存到MySQL

之所以不适用 spark 直接写 MySQL 的原因，是因为 spark 写 MySQL 的数据类型不是我们想要的，而且不能创建索引等等。

如果使用 append 的方式写入，还是要先在 spark 程序外面用代码清空 MySQL 的表数据，还不如这里写个函数方便呢。

注意，
1、这里需要 MySQL 事先创建好表，而且 MySQL 和 hive 表的字段名称和数量保持一致.
2、默认情况下会先清空MySQL的表，可以通过参数控制

"""
import traceback
from . import pymysql
from . import pyhive

class hive2mysql():
    def __init__(self, mysql_conn=None, hive_conn=None, mysql_query='ai_mysql', hive_query='ai_hive' ):
        """
        如果传入的参数是已经存在的连接，就用已经存在的连接，否则就使用快速连接的方式连接数据库
        :param mysql_conn: 已经连接mysql的对象
        :param hive_conn: 已经连接hive的对象
        :param mysql_query: 需要快速连接哪个MySQL数据库
        :param hive_query: 需要快速连接哪个hive数据库
        """
        self._mysql_conn=None
        self._hive_conn=None
        self._mysql_query=mysql_query
        self._hive_query=hive_query
        self._init()

    def _init(self):
        """初始化数据库连接"""
        # 连接mysql
        if not self._mysql_conn:
            self._mysql_conn = pymysql.mysql(query=self._mysql_query)
            print(self._mysql_conn.read_table(sql='select 1 as mysql'))
            print('连接mysql成功')
        # 连接hive
        if not self._hive_conn:
            self._hive_conn = pyhive.pyhive(query=self._hive_query)
            print(self._hive_conn.read_table(sql='select 1 as hive'))
            print('连接hive成功')

    def hive_data_to_mysql(self, mysql_tb_name, hive_tb_name=None, hive_sql=None, truncate_mysql_tb=True):
        """
        读取数据，保存到mysql.
        读取hive数据的时候，可以传入表名，也可以传入具体的查询SQL
        :param mysql_tb_name:
        :param hive_tb_name:
        :param hive_sql:
        :param truncate_mysql_tb: 数据导入MySQL之前是否先清空mysql表
        :return:
        """
        # 读取hive数据
        hive_tb = self._hive_conn.read_table(tb_name=hive_tb_name, sql=hive_sql)    # 这种方式比较安全
        #
        # 先清空mysql数据
        mode = 'truncate' if truncate_mysql_tb else 'insert'
        # 判断连接是否正常
        try:
            print(self._mysql_conn.read_table(sql='select 1 as mysql'))
        except:
            self._mysql_conn = pymysql.mysql(query=self._mysql_query)
        # 数据入库
        ok, error = self._mysql_conn.df_into_db(tb_name=mysql_tb_name, df=hive_tb, types=mode)
        if not ok:
            raise Exception(error)
        print('将hive的表导入到mysql成功')
