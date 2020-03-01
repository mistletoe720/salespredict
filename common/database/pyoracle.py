# -*- coding: utf-8 -*-
"""
python连接oracle
说明：
这里用普通函数方式和用类的方式这两种方式分别实现了python连接oracle的应用。
至于哪个方便看情况。
需要注意的是，不管用哪种方式，最后都要记得断开与数据库的连接

实现数据库连接池的方式有两种，分别写在两个类中，一种方式是用cx_oracle自带的方法实现，另一个是用DBUtils模块实现。

修改记录：
2016-06-17：苏振裕，修改df_into_oracle函数

"""
# 如果出现读取中文乱码，请在具体模块中添加下面这一句
# os.environ['NLS_LANG'] = 'SIMPLIFIED CHINESE_CHINA.UTF8'

# 动态删表的存储过程
# CREATE OR REPLACE PROCEDURE drop_table_if_exists( table_name varchar2) is
# BEGIN
#  EXECUTE IMMEDIATE 'drop table '||table_name;
# commit;
# exception when others then null;
# END drop_table_if_exists;

import datetime
import cx_Oracle
import pandas as pd
import base64
import traceback

# 获取数据库连接参数

"""
创建数据库连接池
方法1：
pool = PooledDB(creator=cx_Oracle, mincached=1, maxcached=20, user=user, password=pwd, dsn="192.168.1.16/test"):
pool.connection()

方法2：
pool = cx_Oracle.SessionPool("data_source", "data_source", "192.168.1.16:1521/test", 1, 6,2)
con11 = pool.acquire()
sql = "select 1 from dual"
a = pd.read_sql(sql,conn)
pool.release(con11)
"""


class orcl_pool:
    """oracle数据库连接池"""
    pools = None


class pyoracle():
    """数据库连接类"""

    def __init__(self, host=None, port=None, user=None, passwd=None, dsn=None, db='ai', encrypt=True):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.db = db
        self.dsn = dsn
        self.dsn2 = host + "/" + dsn
        self.encrypt = encrypt  # 账号密码是否加密
        self.conn = None
        self.cursor = None
        self._init_pool()

    def _init_pool(self):
        """初始化数据库连接池"""
        if self.encrypt:
            decode = lambda string: base64.b64decode(string).decode()
            host, port, user, passwd = decode(self.host), decode(self.port), decode(self.user), decode(self.passwd)
        else:
            host, port, user, passwd = self.host, self.port, self.user, self.passwd
        self.dsn = self.host + '/' + self.db
        port = int(port)
        if not orcl_pool.pools:
            orcl_pool.pools = cx_Oracle.SessionPool(
                user=self.user, password=self.passwd, dsn=self.dsn2, min=1, max=30, increment=2)

    def get_conn(self):
        """获取数据库连接"""
        return orcl_pool.pools.acquire()

    def release(self, conn):
        """将连接返回连接池"""
        orcl_pool.pools.release(self.conn)

    def close(self):
        """关闭连接"""
        self.cursor.close()
        self.conn.close()

    def get_conn_num(self):
        """获取当前的激活的连接数"""
        return orcl_pool.pools.busy

    def execute(self, sql):
        """执行sql语句"""
        try:
            conn = self.get_conn()
            cur = conn.cursor()
            cur.execute(sql)
            conn.commit()
            ok, error = 1, ''
        except:
            error = traceback.format_exc()
            conn.rollback()
            ok, error = 0, error
        finally:
            cur.close()
            conn.close()
        return ok, error

    def read_table(self, tb_name=None, sql=None):
        """读取数据"""
        if tb_name:
            sql = "select * from %s" % tb_name
        else:
            sql = sql
        conn = self.get_conn()
        data = pd.read_sql(sql, conn)
        data.columns = [col.lower().strip() for col in data.columns]
        conn.close()
        return data

    def df_into_db(self, tb_name, df, time_col=[], time_format="%Y-%m-%d %H:%M:%S", types='INSERT', each_commit_row=20000):
        """使用cx_oracle将数据导入数据库.
        如果将df转成全部字符串格式，数据库表没有时间字段是没有问题的,就算数据库是数值型，也会自动转成对应格式。
        但是如果数据库表有日期字段，就不行了，需要将对应的列转成日期型，其余为字符串格式，才能顺利导入。
        参数：
        table:目标表名
        df:需要入库的DataFrame，一定是df格式
        time_col:是否包含时间字段，为['A', 'B'...] 格式，就是只有一个元素，也要写成['A']
        time_format:将字符串转车日期类型的格式，其实pandas会自动判断，是不需要的
        type: 插入数据之前是否需要清空表,insert不需要,truncate需要
        cols：为保持函数向后兼容，才保留这个参数的
        """
        # 如果是空的数据框，什么都不做
        if len(df) == 0:
            return 1, ''
        # 判断是否需要挺空表
        if types.upper() in "TRUNCATE,DELETE":
            truncate_sql = "TRUNCATE FROM " + tb_name
            self.execute(sql=truncate_sql)
        # 拼接入库SQL
        sql = "insert into table_name ( columns ) values(:ints)"
        columns = ','.join(df.columns)
        numstr = [str(i) for i in range(1, len(df.columns) + 1)]
        ints = ":" + ',:'.join(numstr)  # 得到':1,:2,:3,:4,:5,:6,:7'的字符串
        sql = sql.replace('table_name', tb_name)
        sql = sql.replace('columns', columns)
        sql = sql.replace(':ints', ints)
        # 分批次入库
        try:
            conn = self.get_conn()
            cur = conn.cursor()
            cnts = len(df)
            each_cnt = each_commit_row
            for i in range(0, cnts, each_cnt):
                # 取出子集，全部转成字符串格式，主要是针对数值，df中的时间此时肯定是字符串类型
                # 处理空值问题，因为刚才转成字符串时，None被转成"None",对于原来是NaN的，要转成空字符串
                df2 = df.iloc[i:i + each_cnt].applymap(lambda s: str(s))
                nan_df = df.notnull()
                df2 = df2.where(nan_df, None)  # 转成空字符串或空值
                # 如果是日期类型字符串，要转成日期类型
                if len(time_col) > 0:
                    for col in time_col:
                        df2[col] = df2[col].apply(lambda s: pd.to_datetime(s))
                # 将df转成list-tuple格式，才能导入数据库
                param = df2.to_records(index=False).tolist()
                # 入库
                cur.prepare(sql)
                cur.executemany(None, param)
                conn.commit()
                # 耗时清空
                t = str(datetime.datetime.now())[:19]
                print('%s data of [ %.2fw - %.2fw ) /%d into %s' % (t, i / 10000, (i + each_cnt) / 10000, cnts, tb_name))
            # 正常关闭
            ok, error = 1, ''
        except:
            ok, error = 0, traceback.format_exc()
            conn.rollback()
        finally:
            cur.close()
            conn.close()
        return ok, error


def _test():
    # 连接数据库，注意必须传入dsn
    pyorcl = pyoracle(host='localhost', port='1521', user='hr', passwd='123456',
                      db='hr', dsn='XE', encrypt=False)
    # 查询
    pyorcl.read_table(sql='select 1 from dual')
    # 测试数据插入
    sql = """
    create table test_tb (id int, name varchar2(50), score number(10,2) )    
    """
    pyorcl.execute(sql)
    #
    df = pd.DataFrame({'id': [1, 2, 3], 'name': ['a', 'b', 'c'], 'score': [1.3, None, 5.5]})
    pyorcl.df_into_db(tb_name='test_tb', df=df)
