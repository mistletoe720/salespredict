# -*-coding: utf-8-*-
# File   : pool.py
# Version: 0.1
# Description: connection pool manager.
import traceback
import base64
import pandas as pd
import datetime
from copy import copy
import time
from collections import defaultdict
import logging
import threading
import contextlib
from pymysql.connections import Connection
from pymysql.cursors import DictCursor, Cursor
from queue import Queue, Empty

logger = logging.getLogger('pymysqlpool')


class NoFreeConnectionFoundError(Exception):
    pass


class PoolBoundaryExceedsError(Exception):
    pass


class PoolIsFullException(Exception):
    pass


class PoolIsEmptyException(Exception):
    pass


class PoolContainer(object):
    """
    Pool container class: it's a pool manager with safe threading locks.
    Be aware of the dead lock!!!!!!!!!!!
    """

    def __init__(self, max_pool_size):
        self._pool_lock = threading.RLock()
        self._free_items = Queue()
        # self._pool_items = list()
        self._pool_items = set()
        self._max_pool_size = 0
        self.max_pool_size = max_pool_size

    def __repr__(self):
        return '<{0.__class__.__name__} {0.size})>'.format(self)

    def __iter__(self):
        with self._pool_lock:
            return iter(self._pool_items)

    def __contains__(self, item):
        with self._pool_lock:
            return item in self._pool_items

    def __len__(self):
        with self._pool_lock:
            return len(self._pool_items)

    def add(self, item):
        """Add a new item to the pool"""
        # Duplicate item will be ignored
        if item is None:
            return None

        if item in self:
            logger.debug(
                'Duplicate item found "{}", '
                'current size is "{}"'.format(item, self.size))
            return None

        if self.pool_size >= self.max_pool_size:
            raise PoolIsFullException()

        self._free_items.put_nowait(item)
        with self._pool_lock:
            # self._pool_items.append(item)
            self._pool_items.add(item)

        logger.debug(
            'Add item "{!r}",'
            ' current size is "{}"'.format(item, self.size))

    def return_(self, item):
        """Return a item to the pool. Note that the item to be returned should exist in this pool"""
        if item is None:
            return False

        if item not in self:
            logger.error(
                'Current pool dose not contain item: "{}"'.format(item))
            return False

        self._free_items.put_nowait(item)
        logger.debug('Return item "{!r}", current size is "{}"'.format(item, self.size))
        return True

    def get(self, block=True, wait_timeout=60):
        """Block until a free item is found in `wait_timeout` seconds.
        Otherwise, a `WaitTimeoutException` will be raised.
        If `wait_timeout` is None, it will block forever until a free item is found.
        """
        try:
            item = self._free_items.get(block, timeout=wait_timeout)
        except Empty:
            raise PoolIsEmptyException('Cannot find any available item')
        else:
            logger.debug('Get item "{}",'
                         ' current size is "{}"'.format(item, self.size))
            return item

    @property
    def size(self):
        # Return a tuple of the pool size in detail
        return '<max={}, current={}, free={}>'.format(self.max_pool_size, self.pool_size, self.free_size)

    @property
    def max_pool_size(self):
        return self._max_pool_size

    @max_pool_size.setter
    def max_pool_size(self, value):
        if value > self._max_pool_size:
            self._max_pool_size = value

    @property
    def pool_size(self):
        return len(self)

    @property
    def free_size(self):
        """Not reliable as described in document of the `queue` module"""
        return self._free_items.qsize()


class MySQLConnectionPool(object):
    """
    A connection pool manager.
    """

    def __init__(self, pool_name, host=None, user=None, password="", database=None, port=3306,
                 charset='utf8', use_dict_cursor=True, max_pool_size=30,
                 enable_auto_resize=True, auto_resize_scale=1.5,
                 pool_resize_boundary=48,
                 defer_connect_pool=False, **kwargs):

        """
        Initialize the connection pool.

        Update: 2017.06.19
            1. remove `step_size` argument
            2. remove `wait_timeout` argument

        :param pool_name: a unique pool_name for this connection pool.
        :param host: host to your database server
        :param user: username to your database server
        :param password: password to access the database server
        :param database: select a default database(optional)
        :param port: port of your database server
        :param charset: default charset is 'utf8'
        :param use_dict_cursor: whether to use a dict cursor instead of a default one
        :param max_pool_size: maximum connection pool size (max pool size can be changed dynamically)
        :param enable_auto_resize: if set to True, the max_pool_size will be changed dynamically
        :param pool_resize_boundary: !!this is related to the max connections of your mysql server!!
        :param auto_resize_scale: `max_pool_size * auto_resize_scale` is the new max_pool_size.
                                The max_pool_size will be changed dynamically only if `enable_auto_resize` is True.
        :param defer_connect_pool: don't connect to pool on construction, wait for explicit call. Default is False.
        :param kwargs: other keyword arguments to be passed to `pymysql.Connection`
        """
        # config for a database connection
        self._host = host
        self._user = user
        self._password = password
        self._database = database
        self._port = port
        self._charset = charset
        self._cursor_class = DictCursor if use_dict_cursor else Cursor
        self._other_kwargs = kwargs

        # config for the connection pool
        self._pool_name = pool_name
        self._max_pool_size = max_pool_size if max_pool_size < pool_resize_boundary else pool_resize_boundary
        # self._step_size = step_size
        self._enable_auto_resize = enable_auto_resize
        self._pool_resize_boundary = pool_resize_boundary
        if auto_resize_scale < 1:
            raise ValueError(
                "Invalid scale {}, must be bigger than 1".format(auto_resize_scale))

        self._auto_resize_scale = int(round(auto_resize_scale, 0))
        # self.wait_timeout = wait_timeout
        self._pool_container = PoolContainer(self._max_pool_size)

        self.__safe_lock = threading.RLock()
        self.__is_killed = False
        self.__is_connected = False

        if not defer_connect_pool:
            self.connect()

    def __repr__(self):
        return '<MySQLConnectionPool ' \
               'name={!r}, size={!r}>'.format(self.pool_name, self.size)

    def __del__(self):
        self.close()

    def __iter__(self):
        """Iterate each connection item"""
        return iter(self._pool_container)

    @property
    def pool_name(self):
        return self._pool_name

    @property
    def pool_size(self):
        return self._pool_container.pool_size

    @property
    def free_size(self):
        return self._pool_container.free_size

    @property
    def size(self):
        return '<boundary={}, max={}, current={}, free={}>'.format(self._pool_resize_boundary,
                                                                   self._max_pool_size,
                                                                   self.pool_size,
                                                                   self.free_size)

    @contextlib.contextmanager
    def cursor(self, cursor=None):
        """Shortcut to get a cursor object from a free connection.
        It's not that efficient to get cursor object in this way for
        too many times.
        """
        with self.connection(True) as conn:
            assert isinstance(conn, Connection)
            cursor = conn.cursor(cursor)
            try:
                yield cursor
            except Exception as err:
                conn.rollback()
                raise err
            finally:
                cursor.close()

    @contextlib.contextmanager
    def connection(self, autocommit=False):
        conn = self.borrow_connection()
        assert isinstance(conn, Connection)
        old_value = conn.get_autocommit()
        conn.autocommit(autocommit)
        try:
            yield conn
        except Exception as err:
            # logger.error(err, exc_info=True)
            raise err
        finally:
            conn.autocommit(old_value)
            self.return_connection(conn)

    def connect(self):
        """Connect to this connection pool
        """
        if self.__is_connected:
            return

        logger.info('[{}] Connect to connection pool'.format(self))

        test_conn = self._create_connection()
        try:
            test_conn.ping()
        except Exception as err:
            raise err
        else:
            with self.__safe_lock:
                self.__is_connected = True

            self._adjust_connection_pool()
        finally:
            test_conn.close()

    def close(self):
        """Close this connection pool"""
        try:
            logger.info('[{}] Close connection pool'.format(self))
        except Exception:
            pass

        with self.__safe_lock:
            if self.__is_killed is True:
                return True

        self._free()

        with self.__safe_lock:
            self.__is_killed = True

    def borrow_connection(self):
        """
        Get a free connection item from current pool. It's a little confused here, but it works as expected now.
        """
        block = False

        while True:
            conn = self._borrow(block)
            if conn is None:
                block = not self._adjust_connection_pool()
            else:
                return conn

    def _borrow(self, block):
        try:
            connection = self._pool_container.get(block, None)
        except PoolIsEmptyException:
            return None
        else:
            # check if the connection is alive or not
            connection.ping(reconnect=True)
            return connection

    def return_connection(self, connection):
        """Return a connection to the pool"""
        return self._pool_container.return_(connection)

    def _adjust_connection_pool(self):
        """
        Adjust the connection pool.
        """
        # Create several new connections
        logger.debug('[{}] Adjust connection pool, '
                     'current size is "{}"'.format(self, self.size))

        if self.pool_size >= self._max_pool_size:
            if self._enable_auto_resize:
                self._adjust_max_pool_size()

        try:
            connection = self._create_connection()
        except Exception as err:
            logger.error(err)
            return False
        else:
            try:
                self._pool_container.add(connection)
            except PoolIsFullException:
                # logger.debug('[{}] Connection pool is full now'.format(self.pool_name))
                return False
            else:
                return True

    def _adjust_max_pool_size(self):
        with self.__safe_lock:
            self._max_pool_size *= self._auto_resize_scale
            if self._max_pool_size > self._pool_resize_boundary:
                self._max_pool_size = self._pool_resize_boundary
            logger.debug('[{}] Max pool size adjusted to {}'.format(self, self._max_pool_size))
            self._pool_container.max_pool_size = self._max_pool_size

    def _free(self):
        """
        Release all the connections in the pool
        """
        for connection in self:
            try:
                connection.close()
            except Exception as err:
                _ = err

    def _create_connection(self):
        """Create a pymysql connection object
        """
        return Connection(host=self._host,
                          user=self._user,
                          password=self._password,
                          database=self._database,
                          port=self._port,
                          charset=self._charset,
                          cursorclass=self._cursor_class,
                          **self._other_kwargs)


############################################################################################################################################
# 上面是第三方库的代码，下面才是我写的代码
# 构建全局的数据库连接池
_db_pool = defaultdict()


class pymysql():
    def __init__(self, host, port, user, passwd, db='', encrypt=True, pool_size=10):
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.db = db
        self.encrypt = encrypt
        self.pool_size = pool_size
        self._conn = None
        self.pool_key = '%s:%s:%s' % (str(host), str(port), db)  # 区分不同实例数据库连接池需要的名称
        self._init_pool()

    def _init_pool(self):
        """初始化连接池"""
        # 如果已经创建，就直接返回
        if _db_pool.get(self.pool_key, ''):
            return
        # 如果没有，就创建连接池
        host, port, user, passwd = self.host, self.port, self.user, self.passwd
        if self.encrypt:
            decode = lambda string: base64.b64decode(string).decode()
            host, port, user, passwd = decode(host), decode(port), decode(user), decode(passwd)
        port = int(port)
        pool = MySQLConnectionPool(pool_name='ai', host=host, port=port, user=user, password=passwd, database=self.db, max_pool_size=self.pool_size)
        _db_pool[self.pool_key] = pool

    def execute(self, sql, retry=1):
        """直接访问并获取一个 cursor 对象，自动 commit 模式会在这种方式下启用"""
        i = 1
        while i <= retry:
            try:
                with _db_pool[self.pool_key].cursor() as cursor:
                    cursor.execute(sql)
                    ok, error = 1, ''
                    return ok, error
            except:
                ok = 0
                error = traceback.format_exc()
                time.sleep(1)
                print('='*200)
                print('第%d次执行SQL失败，等待1秒后重试：%s' % (i, sql))
                print(error.replace('\n','\n||'))
                print('='*200)
            i += 1
        # 如果三次执行失败，就返回错误
        return ok, error

    @property
    def conn(self):
        """获取连接"""
        self._conn = _db_pool[self.pool_key].borrow_connection()
        return self._conn

    def close(self):
        """关闭连接"""
        _db_pool[self.pool_key].return_connection(self._conn)

    def free(self):
        """释放所有连接"""
        _db_pool[self.pool_key].close()

    def read_table(self, tb_name=None, sql=None):
        """读取表数据，如果传入表名则直接读取表数据，否则按照sql来读"""
        if not sql:
            sql = "select * from {tb_name}".format(tb_name=tb_name)
        with _db_pool[self.pool_key].connection() as conn:
            data = pd.read_sql(sql, conn)
        # 字段名转小写
        data.columns = [col.lower().replace(' ', '').split('.')[-1] for col in data.columns]
        return data

    def read_big_table(self, tb_name=None, sql=None, each_fetch_size=100000):
        """
        读取大表数据，在读取大表数据时，由于超时或者数据量过大导致连接时效的问题。
        针对这个问题，可以分批次读取，以及设置最大传输量等。
        """
        # info = """read_big_table 函数用于读取MySQL大表数据，因为mysql可能会出现数据量过大而超时，溢出等异常，因此需要特殊处理。
        # 方法有：conn.max_allowed_packet=67108864. 不是一次读取全部数据，而是分批次一次读取10w ...等 """
        # 获取SQL
        if not sql:
            sql = "select * from {tb_name}".format(tb_name=tb_name)
        # 第一步：获取表头字段名
        cols_sql = sql + ' limit 1'
        cols = self.read_table(sql=cols_sql).columns.tolist()
        cols = [col.lower().replace(' ', '').split('.')[-1] for col in cols]
        print('获取的字段名是：' + ','.join(cols))
        # 分批次读取
        try:
            data = []
            with _db_pool[self.pool_key].cursor() as cursor:
                cursor.execute(sql)
                i = 0
                while True:
                    sub_data = [row for row in cursor.fetchmany(each_fetch_size)]
                    if len(sub_data) > 0:
                        data.extend(sub_data)
                        t = str(datetime.datetime.now())[:19]
                        print('%s:读取数据 [%d-%d)' % (t, i, (i + len(sub_data))))
                        i += len(sub_data)
                    else:
                        break
        except:
            error = traceback.format_exc()
        # 转成dataframe，返回
        data2 = pd.DataFrame(data, columns=cols)
        return data2

    def dict_into_db(self, tb_name, data):
        """
        将字典类型的数据导入数据库。这个在参数服务器，日志表等方面很有用。
        需要注意的是，这里使用MySQL的写法，具体数据库需要另外实现改方法。
        """
        cols = ','.join([k for k, v in data.items()])
        # 注意，下面需要使用单引号引住双引号，因为值可能有单引号啊
        values = '","'.join([str(v) for k, v in data.items()])
        values = '"' + values + '"'
        sql = "insert into {tb} ({cols}) values ({values})".format(tb=tb_name, cols=cols, values=values)
        code, error = self.execute(sql)
        return code, error

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
        # 判断是否需要清空表
        if types.upper() in "TRUNCATE,DELETE":
            truncate_sql = "delete from " + tb_name
            print('将清空数据库表：%s' % truncate_sql)
            ok, error = self.execute(truncate_sql)
            if not ok:
                raise Exception('清空表步骤出错，下面是错误提示：%s' % error)

        # 创建入库的sql
        sql = "insert into tb_name (cols) values (%s_many_times) "
        cols = list(df.columns)
        cols_string = ', '.join(cols)
        num = len(list(df.columns))
        num_string = ','.join(['%s' for i in range(num)])

        sql = sql.replace('tb_name', tb_name)
        sql = sql.replace('cols', cols_string)
        sql = sql.replace('%s_many_times', num_string)

        # 入库,每次提交2w
        try:
            cnts = len(df)
            each_cnt = each_commit_row
            for i in range(0, cnts, each_cnt):
                # 取出子集，全部转成字符串格式，主要是针对数值，df中的时间此时肯定是字符串类型
                # 处理空值问题，因为刚才转成字符串时，None被转成"None",对于原来是NaN的，要转成空字符串
                df2 = df.iloc[i:i + each_cnt].applymap(lambda s: str(s))
                nan_df = df.notnull()
                df2 = df2.where(nan_df, None)  # 转成空字符串或空值
                # 将df转成list-tuple格式，才能导入数据库
                param = df2.to_records(index=False).tolist()
                with _db_pool[self.pool_key].cursor() as cursor:
                    cursor.executemany(sql, param)
                t = str(datetime.datetime.now())[:19]
                print('%s insert data of [ %.2fw - %.2fw ) /%d rows into %s' %
                      (t, i / 10000, (i + each_cnt) / 10000, cnts, tb_name))
        except:
            error = traceback.format_exc()
            return 0, error
        return 1, ''

    def get_param(self, pkey='pkey', pvalue='pvalue', tb_name='da_supplychain_params_server'):
        """
        参数服务器：从数据库读取参数值.
        :param pkey: 具体的某一个键名
        :param pvalue: 要查询的字段值，如果是string，那么查询单字段，如果是list，那么查询多字段
        :param tb_name: 参数表名
        :return:
        """
        pvalue2 = [pvalue] if isinstance(pvalue, str) else pvalue
        pvalue2 = ','.join(pvalue2)
        sql = "select %s from %s where pkey='%s' " % (pvalue2, tb_name, pkey)
        data = self.read_table(sql=sql)
        # 没数据返回None
        if len(data) == 0:
            print('参数 pkey=%s 不存在，请检查' % pkey)
            return None
        # 有数据就返回
        if isinstance(pvalue, str):
            return data[pvalue].iat[0]
        else:
            return data[pvalue].to_dict(orient='record')[0]

    def update_param(self, pkey='', pvalue='', tb_name='da_supplychain_params_server'):
        """
        参数服务器：更新参数。首先要确保参数是存在的，不然就插入。需要对pkey建立索引。
        如果pvalue是字典类型，则更新多字段，否则更新一个字段
        """
        # 判断是否存在
        has_been = self.get_param(pkey=pkey, tb_name=tb_name)
        # 如果存在则更新
        if has_been:
            # update部分
            if isinstance(pvalue, dict):
                updates = ' , '.join(["%s='%s'" % (k.lower().strip(), v.lower().strip()) for k, v in pvalue.items()])
            else:
                updates = " pvalue='%s' " % pvalue.lower().strip()
            # where部分
            condition = " pkey='%s' " % pkey.lower().strip()
            # 全部
            sql = "update {tb} set {updates} where {condition}".format(tb=tb_name, updates=updates, condition=condition)
        # 不存在则插入
        else:
            print('将会把参数pkey=%s插入到参数服务表,值是：%s' % (str(pkey), str(pvalue)))
            data = {}
            data['pkey'] = pkey.lower().strip()
            if isinstance(pvalue, str):
                data['pvalue'] = pvalue
            if isinstance(pvalue, dict):
                data.update(pvalue)
            #
            sqls = [[col, value] for col, value in data.items()]
            cols = ','.join([c[0] for c in sqls])
            # 注意，下面需要使用单引号引住双引号，因为值可能有单引号啊
            values = '","'.join(str(v[1]) for v in sqls)
            values = '"' + values + '"'
            sql = "insert into {tb} ({cols}) values ({values})".format(tb=tb_name, cols=cols, values=values)
        # 执行插入或者更新操作
        ok, error = self.execute(sql)
        return ok

    def load_data_infile(self, tb_name, file):
        """将本地文件导入到MySQL表"""
        # LOAD DATA INFILE 'persondata.txt' INTO TABLE persondata (col1,col2,...) FIELDS TERMINATED BY ',' LINES TERMINATED BY '\n';
        # 读取表的字段名
        col_sql = "select * from %s limit 1" % tb_name
        tb_cols = self.read_table(sql=col_sql).columns.tolist()
        # 判断file的字段
        with open(file, 'r') as f:
            file_cols = f.readline()[:-1].replace(' ', '').split(',')
        # 判断是否包含
        not_in_col = [col for col in file_cols if col not in tb_cols]
        if len(not_in_col) > 0:
            raise Exception('文件中的字段和表的字段不一致，请检查，%s在file中而不在table中' % ','.join(not_in_col))
        # 接下来执行cmd
        # 启动sql
        host, port, user, passwd = self.host, self.port, self.user, self.passwd
        if self.encrypt:
            decode = lambda string: base64.b64decode(string).decode()
            host, port, user, passwd = decode(host), decode(port), decode(user), decode(passwd)
        sql1 = "mysql -h%s -u%s -p%s" % (host, user, passwd)
        # load_data
        to_tb_cols = ','.join(file_cols)
        sql2 = "load data infile '%s' into table %s(%s) fields terminated by ',' lines terminated by '\n'; " % (file, tb_name, to_tb_cols)
        #
        cmd = """ %s %s  """ % (sql1, sql2)
        print(cmd.replace('\n', '\\n'))
        t1 = datetime.datetime.now()
        # done = os.system(cmd)
        done, error = self.execute(sql2)
        t2 = datetime.datetime.now()
        if done == 0:
            print('数据导入到数据库成功，耗时：%d' % (t2 - t1).seconds)
        else:
            print('数据导入失败\n' + error)

    def delete_old_data(self, tb_name, value, col='statedate',
                        keepdays=None, delete_today=False,
                        dateformat='%Y-%m-%d'):
        """
        删除表中超过多少天的数据或者或者指定统计日期当天的数据。
        这里有个基本假设，就是日期的字段名叫statedate,日期都是字符串，格式都是%Y-%m-%d
        如果 keepday>0，表示要删除以往的数据
        如果 keepday=0或者None，表示删除指定日期当天数据
        """
        # 判断是否是删除今天的数据
        # keepdays=None,表示删除具体某天的数据,此时 delete_today 无效
        if not keepdays:
            sql = "delete from {tb} where {col}='{value1}'".format(tb=tb_name, col=col, value1=value)
        # 删除今天的数据，以及删除历史太久远的数据
        elif keepdays and delete_today:
            lastday = (pd.to_datetime(value) - datetime.timedelta(keepdays)).strftime(dateformat)
            sql = "delete from {tb} where {col}<='{value1}' or {col}='{value2}'". \
                format(tb=tb_name, col=col, value1=lastday, value2=value)
        # 只删除历史久远数据
        else:
            lastday = (pd.to_datetime(value) - datetime.timedelta(keepdays)).strftime(dateformat)
            sql = "delete from {tb} where {col}<='{value1}'".format(tb=tb_name, col=col, value1=lastday)
        # 执行
        print('将删除表数据：' + sql)
        ok, error = self.execute(sql)
        return ok

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


def test():
    # 连接本地数据库
    conn = pymysql('localhost', '3307', 'u', '123456', 'test', False)
    #
    # 执行非select返回型的SQL
    sql = "create table test2 as select * from student1"
    ok, error = conn.execute(sql)
    #
    sql = "insert into student values(1,'a','a')"
    conn.execute(sql)
    #
    # 导入数据
    df = pd.DataFrame()
    ok, error = conn.df_into_db(tb_name='test2', df=df, types='delete')
    #
    # 查询取数
    sql = 'select a from tests'
    data = conn.read_table(sql)
    #
    # 自定义pandas查询，不建议使用，而应该使用conn.read_table，其内部就是用pd.read_sql，
    cc = conn.conn
    data = pd.read_sql('select 1', cc)
    cc.close()
    #
    # 测试参数表
    conn.get_param(pkey='a')
    conn.update_param(pkey='a', pvalue='b')
    conn.update_param(pkey='a', pvalue={'pvalue2': 'a', 'pvalue3': '3'})
    conn.update_param(pkey='b', pvalue={'pvalue2': 'a', 'pvalue3': '3'})
