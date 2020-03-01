# -*- coding: utf-8 -*-
"""
Python与mysql的通信模块，使用连接池的方式，可以执行使用多个连接池连接不同的数据库
"""
import os
import base64
import traceback
import pymysql as _pymysql
import pandas as pd
import datetime
import time
from copy import copy
from collections import defaultdict
from DBUtils.PooledDB import PooledDB
from ...config.config import ex_data

# 构建全局的数据库连接池
_db_pool = defaultdict()

# 获取当前时间，用于打印时的格式化
now_str = lambda: '[%s]' % str(datetime.datetime.now())[:19]


class mysql():
    """MySQL连接对象"""

    def __init__(self, host=None, port=None, user=None, passwd=None, encrypt=True, db='AI',
                 connect_timeout=28800, mincached=0, maxcached=1, maxshared=0, maxconnections=30,
                 query=None, charset='utf8'):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.encrypt = encrypt  # 传进来的host和账号是否已经加密
        self.db = db  # 需要连接哪个DB
        self.connect_timeout = connect_timeout  # 超时时间
        self.mincached = mincached  # 初始化时，连接池至少创建的空闲的连接，0表示不创建
        self.maxcached = maxcached  # 连接池空闲的最多连接数，0和None表示没有限制
        self.maxconnections = maxconnections  # 连接池允许的最大连接数，0和None表示没有限制
        self.maxshared = maxshared  # 连接池中最多共享的连接数量，0和None表示全部共享
        # ps:其实并没有什么用，因为pymsql和MySQLDB等模块中的threadsafety都为1，所有值无论设置多少，_maxcahed永远为0，所以永远是所有链接共享
        self.pool_key = '%s:%s:%s' % (str(host), str(port), db)  # 区分不同实例数据库连接池需要的名称
        self.charset = charset  # 字符集，默认utf8，有时候要utf8mb4才行
        self.query = query  # 查询哪个目标数据库，如果传入这个参数，那么host等参数都可以不要传入，进行快速连接
        self._init_pool()  # 创建数据库连接池

    def _init_pool(self):
        """创建数据库连接池"""
        # 增加快速链接，直接从config读取参数，而不需要特意先读取参数再连接
        if self.query == 'mysql':
            self.host, self.port, self.user, self.passwd = mysql
            self.db = 'AI'
        elif self.query == 'bi_mysql':
            self.host, self.port, self.user, self.passwd = mysql
            self.db = 'sqlsource'
        elif self.query == 'azkaban':
            self.host, self.port, self.user, self.passwd = azakaban
            self.db = 'azkaban'
        self.pool_key = '%s:%s:%s' % (str(self.host), str(self.port), self.db)
        #
        # 如果已经创建，就直接返回
        if _db_pool.get(self.pool_key, ''):
            pass
        # 否则创建连接池
        host, port, user, passwd = self.host, self.port, self.user, self.passwd
        if self.encrypt:
            decode = lambda string: base64.b64decode(string).decode()
            host, port, user, passwd = decode(host), decode(port), decode(user), decode(passwd)
        port = int(port)
        # 失败重连
        cnt = 1
        while cnt < 10:
            try:
                _db_pool[self.pool_key] = PooledDB(
                    creator=_pymysql, host=host, port=port, user=user, passwd=passwd, db=self.db,
                    connect_timeout=self.connect_timeout, charset=self.charset, local_infile=1,
                    mincached=self.mincached, maxcached=self.maxcached, maxconnections=self.maxconnections, maxshared=self.maxshared)
                return
            except:
                error = traceback.format_exc()
                print(now_str(), '连接数据库失败，等待3秒后重试第%d次连接' % cnt)
                cnt += 1
        # 如果执行到这一步，说明重试cnt次后还是失败
        raise Exception('连接数据库失败：\n' + error)

    def get_conn(self, only_conn=True):
        """从数据库获取一个连接"""
        conn = _db_pool.get(self.pool_key).connection()
        if only_conn:
            return conn
        else:
            cur = conn.cursor()
            return conn, cur

    @property
    def conn(self):
        """返回数据量连接。之所以有这个，是因为有些就代码直接使用类的conn属性，在使用连接池后，这个属性没有了，为了兼容而增加"""
        return self.get_conn()

    def close(self, conn=None, cur=None):
        """关闭数据库连接"""
        if cur:
            cur.close()
        if conn:
            conn.close()

    def pretty_error(self, error):
        """没别的意思，就是美化下error的错误输出而已"""
        error = ['||' + line for line in error.split('\n') if line]
        error = '%s\n%s\n%s' % ('=' * 150, '\n'.join(error), '=' * 150)
        return error

    def check(self, conn):
        """检查连接"""
        conn.ping()
        pd.read_sql('selct 1')
        return conn

    def execute(self, sql):
        """执行非select型的SQL，没有返回值，主要用于增删改的操作。"""
        i = 1
        while i <= 10:
            conn = self.get_conn()
            cur = conn.cursor()
            # conn, cur = self.get_conn(only_conn=False)
            # conn.ping()
            try:
                cur.execute(sql)
                cur.execute('commit')
                # conn.commit()
                self.close(conn, cur)
                ok, error = 1, ''
            except:
                conn.rollback()  # 回滚操作
                error = traceback.format_exc()
                last_error_code = [code for code in error.split('\n') if code.strip() != ''][-1]  # 取出最后一行错误信息
                ok, error = 0, self.pretty_error(error)  # 格式化错误信息
            finally:
                # self.close(conn, cur)
                cur.close()
                conn.close()
            # 判断是否成功
            if ok:
                return ok, error
            else:
                print(now_str(), '尝试第 %d 次执行sql失败，错误代码: %s，等待3秒后再次尝试执行sql：%s' % (i, last_error_code, sql))
                time.sleep(3)
            #
            i += 1
        # 达到最大重试次数仍然失败
        print(now_str(), '重试 %d 次后仍然失败，不再尝试执行sql：%s' % (i - 1, sql))
        return 0, error

    def read_table(self, tb_name=None, sql=None):
        """读取表数据，如果传入表名则直接读取表数据，否则按照sql来读"""
        # print('-' * 80)
        if not sql:
            sql = "select * from {tb_name}".format(tb_name=tb_name)
        conn = self.get_conn()
        # t1 = datetime.datetime.now()
        # print('sql='+sql)
        data = pd.read_sql(sql, conn)
        data.columns = [col.lower().split('.')[-1] for col in data.columns]
        # t2 = datetime.datetime.now()
        # print('读取MySQL数据，数据量 %d，耗时 %d 秒，SQL：\n%s' % (len(data), (t2 - t1).seconds, sql))
        # print('-' * 80)
        self.close(conn)
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
        conn, cur = self.get_conn(only_conn=False)
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
                    print(now_str(), '%s:读取数据 [%d-%d)' % (t, i, (i + len(sub_data))))
                    i += len(sub_data)
                else:
                    break
        except:
            error = traceback.format_exc()
            self.close(conn, cur)  # 返回错误之前，记得先关闭连接
            raise Exception(self.pretty_error(error))
        # 如果正常，记得关闭cursor
        self.close(conn, cur)
        # 转成dataframe
        data2 = pd.DataFrame(data, columns=cols)
        # print('-' * 80)
        return data2

    def dict_into_db(self, tb_name, data):
        """
        将字典类型的数据导入数据库。这个在参数服务器，日志表等方面很有用。
        需要注意的是，这里使用MySQL的写法，具体数据库需要另外实现改方法。
        """
        # data={'id': 1, 'name': 'myname', 'score':98.5}
        cols = ','.join([k for k, v in data.items()])
        # 注意，下面需要使用单引号引住双引号，因为值可能有单引号啊
        values = '","'.join([str(v) for k, v in data.items()])
        values = '"' + values + '"'
        sql = "insert into {tb} ({cols}) values ({values})".format(tb=tb_name, cols=cols, values=values)
        code, error = self.execute(sql)
        return code, error

    def write_log(self, data={}, tb='da_supplychain_batch_log'):
        """将dict类型的data写到日志表.功能和dict_into_db一样，只是为了方便识别而已"""
        code, error = self.dict_into_db(tb, data)
        return code, error

    def get_param(self, pkey='', pvalue='pvalue', tb_name='da_supplychain_params_server'):
        """
        参数服务器：从数据库读取参数值.
        :param pkey: 由于一个key可以有多个value，为了程序更加通用，这里插入的key既可以是string，也可以是dict。
                        如果传入dict，那么就是多条件查询更新了
        :param pvalue: 要查询的字段值，如果是string，那么查询单字段，如果是list，那么查询多字段
        :param tb_name: 参数表名
        :return:
        """
        # 组装SQL
        if isinstance(pkey, dict):
            condition = " pkey='%s' " % pkey.pop('pkey'.lower().strip())  # pkey作为索引字段放前面
            condition += ' and ' + ' and '.join(["%s='%s'" % (k.lower().strip(), v.lower().strip())
                                                 for k, v in pkey.items()])
        else:
            condition = " pkey='%s' " % pkey
        if isinstance(pvalue, list):
            value_col = " , ".join(pvalue)
        else:
            value_col = pvalue
        sql = "select {value_col} from {tb} where {condition} ".format(
            value_col=value_col, condition=condition, tb=tb_name)
        # 开始查询
        try:
            param = self.read_table(sql=sql)  # 返回dataframe
            # 如果如果pvalue传入的是list，则返回dict
            return param['pvalue'].iat[0] \
                if isinstance(pvalue, str) \
                else param[pvalue].to_dict(orient='record')[0]
        except:
            print(now_str(), '参数条件：%s 不存在' % condition)
            return None

    def update_param(self, pkey='', pvalue='', tb_name='da_supplychain_params_server'):
        """
        参数服务器：更新参数。首先要确保参数是存在的，不然就插入。需要对pkey建立索引。
        如果pkey是多个字段的联合组成，则传入dict，pkey={'a':1, 'b':2} --> where a=1 and b=2
        如果pvalue是多个字段联合组成，则更新多个字段，pvalue={'a':1, 'b':2} --> update set a=1 and b=2
        """
        # 判断是否存在
        has_been = self.get_param(pkey=copy(pkey), tb_name=tb_name)
        # 如果存在则更新
        if has_been:
            if isinstance(pkey, dict):
                condition = " pkey='%s' " % pkey.pop('pkey'.lower().strip())  # pkey作为索引字段放前面
                condition += ' and ' + ' and '.join(["%s='%s'" % (k.lower().strip(), v.lower().strip())
                                                     for k, v in pkey.items()])
            else:
                condition = " pkey='%s' " % pkey
            if isinstance(pvalue, dict):
                updates = ' and '.join(["%s='%s'" % (k.lower().strip(), v.lower().strip())
                                        for k, v in pvalue.items()])
            else:
                updates = " pvalue='%s' " % pvalue
            sql = "update {tb} set {updates} where {condition}".format(tb=tb_name, updates=updates, condition=condition)
        # 不存在则插入
        else:
            print(now_str(), '将会把参数：%s插入到参数服务表,值是：%s' % (str(pkey), str(pvalue)))
            pkey = {'pkey': pkey.lower().strip()} if isinstance(pkey, str) else pkey
            pvalue = {'pvalue': pvalue.lower().strip()} if isinstance(pvalue, str) else pvalue
            cols, values = '', ''
            for k, v in pkey.items():
                cols += ',' + k.lower().strip()
                values += "," + v.lower().strip()
            for k, v in pvalue.items():
                cols += ',' + k.lower().strip()
                values += "," + v.lower().strip()
            cols = cols[1:]
            values = "'" + values[1:].replace(',', "','") + "'"
            sql = "insert into {tb} ({cols}) values ({values}) ".format(tb=tb_name, cols=cols, values=values)
        code, error = self.execute(sql=sql)
        return code

    def df_into_db(self, tb_name, df, types='insert', each_commit_row=20000):
        """
        将df导入mysql数据库，不需要特别处理日期列，相比oracle还是很方便的.
        注意，这里是以MySQL写的，如果是其他数据库，需要重新实现该方法。
        """
        if len(df) == 0:
            return 1, ''
        # 比较坑的是，数据库是区分大小写的，而库名大写表名小写更坑
        # tb_name='ai.da_sku_season'
        if '.' in tb_name:
            tb_name = tb_name.split('.')[0].upper() + '.' + tb_name.split('.')[1].lower()
        # 获取连接
        conn, cur = self.get_conn(only_conn=False)
        # 判断是否需要清空表
        if types.upper() in "TRUNCATE,DELETE":
            truncate_sql = "TRUNCATE TABLE " + tb_name
            print(now_str(), '将清空数据库表：%s' % truncate_sql)
            try:
                cur.execute(truncate_sql)
                conn.commit()
            except:
                error = traceback.format_exc()
                conn.rollback()
                self.close(conn, cur)  # 记得返回前要先关闭连接
                raise Exception('清空表步骤出错，下面是错误提示：%s' % error)
        #
        # 创建入库的sql
        sql = "insert into tb_name (cols) values (%s_many_times) "
        cols = list(df.columns)
        cols_string = ', '.join(cols)
        num = len(list(df.columns))
        num_string = ','.join(['%s' for i in range(num)])
        #
        sql = sql.replace('tb_name', tb_name)
        sql = sql.replace('cols', cols_string)
        sql = sql.replace('%s_many_times', num_string)
        #
        # 入库,每次提交2w
        df.index=range(len(df))    # 修改索引，防止后面where出现boardcast错误
        try:
            cnts = len(df)
            each_cnt = each_commit_row
            for i in range(0, cnts, each_cnt):
                # 取出子集，全部转成字符串格式，主要是针对数值，df中的时间此时肯定是字符串类型
                # 处理空值问题，因为刚才转成字符串时，None被转成"None",对于原来是NaN的，要转成空字符串
                df2 = df.iloc[i:i + each_cnt].applymap(lambda s: str(s))
                nan_df = df.iloc[i:i + each_cnt].notnull()
                df2 = df2.where(nan_df, None)  # 转成空字符串或空值
                # 将df转成list-tuple格式，才能导入数据库
                param = df2.to_records(index=False).tolist()
                cur.executemany(sql, param)
                conn.commit()
                print(now_str(), 'data of [ %.2fw - %.2fw ) /%d into %s' %
                      (i / 10000, (i + each_cnt) / 10000, cnts, tb_name))
            # 正常关闭连接
            self.close(conn, cur)
            return 1, ''
        except:
            error = traceback.format_exc()
            conn.rollback()
            self.close(conn, cur)
            return 0, self.pretty_error(error)

    def load_data_infile(self, tb_name, file=None, df=None):
        """将本地文件导入到MySQL表"""
        # LOAD DATA INFILE 'persondata.txt' INTO TABLE persondata (col1,col2,...) FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';
        info = """
        使用 mysql loda data local infile 的方式快速导入数据到mysql，基本要求：
        注意，如果传入的是file，要求数据文件是有表头的，
        注意，所有字符型数据里面不要出现 ',' '\t' '\n' 这3个特殊字符，如果出现了，导入的数据基本都是有问题的.
        注意，字符字段存在file中，不需要双引号引起来
        """
        print(info)
        # tb_name='persondata'
        t1 = datetime.datetime.now()
        # 读取表的字段名
        col_sql = "select * from %s limit 1" % tb_name
        tb_cols = self.read_table(sql=col_sql).columns.tolist()
        # 判断是否传入的是dataframe，需要先保存到本地文件
        now = datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
        if not file:
            file = os.path.join(ex_data, 'mysql_load_data_infile_%s_%s.csv' % (tb_name, now))
            df.to_csv(file, index=False)
            file_cols = df.columns.tolist()
        # 如果传进来的是file，则读取第一行表头
        else:
            with open(file, 'r', encoding='utf-8') as f:
                file_cols = f.readline()[:-1].replace(' ', '').replace('\n', '').split(',')
        # 判断是否file中字段是否全部在tb_name中
        not_in_col = [col for col in file_cols if col not in tb_cols]
        if len(not_in_col) > 0:
            raise Exception('文件中的字段和表的字段不一致，请检查，%s在file中而不在table中' % ','.join(not_in_col))
        # 导数SQL
        file2 = file.replace('\\', '/')
        cols = ','.join(file_cols)
        sql = """
        load data LOCAL infile '{file}'  
        into table `{tb}`  
        fields terminated by ','  
        lines terminated by '\n'  
        IGNORE 1 LINES 
        ({cols})
        """.format(file=file2, tb=tb_name, cols=cols)
        print(sql)
        done, error = self.execute(sql)
        t2 = datetime.datetime.now()
        if done == 1:
            print('数据导入到数据库成功，耗时：%d 秒' % (t2 - t1).seconds)
            if now in file:
                os.remove(file)
            return done, error
        else:
            print('数据导入失败\n' + error)
            return done, error

    def load_data_out_file(self, tb_name=None, sql=None, outfile=None, sep=','):
        """将数据导出到文件，使用逗号分隔符"""
        sql="select * from %s"%tb_name if tb_name else sql
        out_sql = """
        %s  into outfile '%s'  fields terminated by '%s' 
        """%(sql, outfile, sep)
        print('\n\n', out_sql, '\n\n')
        # 判断目录是否存在
        path=os.path.dirname(outfile)
        if not os.path.exists(path):
            os.makedirs(path)
            print('指定的目录不存在，已经创建 %s'%path)
        # 指定导出
        done, error = self.execute(out_sql)
        if done==1:
            print('数据成功导出到：%s'%outfile)
        else:
            print('数据导出失败：\n', error)


    def delete_old_data(self, tb_name, value, col='statedate',
                        keepdays=None, delete_today=False,
                        dateformat='%Y-%m-%d'):
        """
        删除表中超过多少天的数据或者或者指定统计日期当天的数据。
        有这个函数是因为后台的基本是报表，格式比较统一，而且经常是整体插入等，插入前需要删除今天的数据，同时表只需要保留近几天的数据。
        还有一个基本假设，就是表的数据不要超过40天，col（statedate）是索引列。
        这里有个基本假设，就是日期的字段名叫statedate,日期都是字符串，格式都是%Y-%m-%d
        如果 keepday>0，表示要删除以往的数据
        如果 keepday=0或者None，表示删除指定日期当天数据
        tb_name='test_tb'
        value='2018-10-16'
        col='statedate'
        keepdays=6
        delete_today=True
        """
        # 所有要删除的日期
        all_need_deleted = []
        # 删除今天的数据
        if delete_today:
            all_need_deleted.append(value)
        # 删除久远的数据
        # 为什么要用循环而不是 statedate>'2018-10-15' 呢，因为state字段是有索引的，等号比大于小于更快
        if keepdays:
            # 看表中有哪些日期的数据，计算哪些是需要删除的
            date_sql = "select distinct %s from %s" % (col, tb_name)
            all_date = self.read_table(sql=date_sql)
            all_date = [str(pd.to_datetime(d))[:10] for d in all_date[col]]
            need_deleted = [d for d in all_date if (pd.to_datetime(value) - pd.to_datetime(d)).days > keepdays]
            all_need_deleted = all_need_deleted + need_deleted
        # 组装SQL
        all_need_deleted2 = " '%s' " % ("', '".join(all_need_deleted))
        sql = "delete from {tb} where {col} in ( {all_value} )".format(tb=tb_name, col=col, all_value=all_need_deleted2)
        print('删除旧的数据：\n' + sql)
        code, error = self.execute(sql)
        return code

    def delete_old_data2(self, tb_name, **kwargs):
        """
        删除表中某个where条件的数据，由kwargs指定.
        比较坑的是，kwargs是字典，无序的，因此不一定能用上索引，或者因为where后的顺序问题，索引不能有效利用起来。
        :param tb_name:
        :param kwargs:
        :return:
        比如要执行：delete from tb where a=1 and b='bbb'
        使用方法：delete_old_data2(tb_name='tb', a=1, b='bbb')
        """
        condition = []
        for k, v in kwargs.items():
            tmp_str = "{k}={v}"
            if isinstance(v, str):
                v = """ '%s' """ % v
            tmp_str = tmp_str.format(k=k, v=v)
            condition.append(tmp_str)
        condition = ' and '.join(condition)
        sql = """delete from {tb} where {condition}""".format(tb=tb_name, condition=condition)
        print('将删除表数据：' + sql)
        done, error = self.execute(sql)
        return done, error

    def back_old_data(self, source_tb, target_tb, startday=None, endday=None, col='statedate'):
        """
        将当前表数据到历史表,要求历史表和当前表的结构相同.
        而且这里假定备份全天的数据，只有一个statedate字段进行区分。
        :param source_tb:
        :param target_tb:
        :param startday:
        :param endday:
        :param col:
        :return:
        """
        # 备份一天
        if startday == endday:
            sql = "insert into {target_tb} select * from {source_tb} where {col}='{startday}'". \
                format(target_tb=target_tb, source_tb=source_tb, col=col, startday=startday)
        # 备份多天
        else:
            sql = "insert into {target_tb} select * from {source_tb} " \
                  "where {col}>='{startday}' and {col}<='{endday}'". \
                format(target_tb=target_tb, source_tb=source_tb, col=col, startday=startday, endday=endday)
        # 执行
        done, error = self.execute(sql)
        return done


def _test():
    # 连接本地mysql
    local = mysql(host='127.0.0.1', port=3306, user='root', passwd='123456', encrypt=False, db='xxljob')  # 非加密方式
    local.read_table(sql='select * from xxljob.xxl_job_qrtz_blob_triggers')

    # 连接远程192mysql
    conn192 = mysql(host='', port='', user='', passwd='',
                    encrypt=True, db='')  # 加密方式
    conn192.read_table(sql='select * from log limit 1')

    # 测试交叉查询，是否能正确管理连接池, 如果报错说明正常
    conn192.read_table(sql='select * from xxljob.xxl_job_qrtz_blob_triggers')
    local.read_table(sql='select * from log limit 1')

    # 测试获取连接供其他程序查询
    conn = local.get_conn()
    # 或者 conn=local.conn
    import pandas as pd
    data = pd.read_sql('select 1', conn)
    conn.close()  # 最后记得要关闭

    # 测试数据入库
    local.df_into_db('table_name', data, types='truncate')

    # 测试参数表
    # 查询参数
    conn192.get_param(pkey='a', pvalue='pvalue', tb_name='params_server')
    # 更新参数
    conn192.update_param(pkey='b', pvalue='bbbc', tb_name='params_server')
    # 多字段决定的参数更新
    # sql=update tb set pvalue=22 where pkey='aa' and pvalue=3
    conn192.update_param(pkey={'pkey': 'aa', 'pvalue': '3'}, pvalue={'pvalue2': '22'},
                         tb_name='params_server')

    # 测试快速连接，指定连接类型，不需要传入host等信息
    conn = mysql(query='mysql')
    conn.read_table(sql='select 1')
