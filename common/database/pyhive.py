# -*- coding: utf-8 -*-
"""
python与hive或impala交互模块.
pip install impyla

# ----------------------------------------------------------------------
方法1：使用impyla
pip install impyla

方法2：使用ibis
ibis其实是依赖impyla，并将impyla合并到ibis中的一个子模块.

安装方法，以centos为例，Ubuntu一直没成功：

yum -y install cyrus-sasl-plain cyrus-sasl-devel cyrus-sasl-gssapi
yum install gcc-c++
pip install thrift-sasl==0.2.1   #0.2.1和hive版本有关
pip install sasl

# impyla
pip install impyla

# ibis
pip install hdfs
pip install ibis-framework


# ibis使用方法
# 1.查询hdfs数据
hdfs = ibis.hdfs_connect(host='127.0.0.1', port=50070)
hdfs.ls('/')
hdfs.ls('/apps/hive/warehouse/tag')
hdfs.get('/apps/hive/warehouse/tag/000000_0', 'parquet_dir')

# 2.查询数据到dataframe
from ibis.impala.api import connect
conn= connect('10.1.101.100', 10001, auth_mechanism='PLAIN', database='a')
conn.exists_table('helloworld')

sql='set mapreduce.job.queuename=a'
conn.raw_sql(sql)
conn.raw_sql("create table tmp_test as select * from info limit 10")
conn.raw_sql("drop table if exists tmp_test")

requete = conn.sql('select * from predict')
df = requete.execute(limit=None)

# ----------------------------------------------------------------------

这里，如果是windows下，则返回None，这样在windows下调试代码也不需要硬编码


"""
import os
import re
import base64
import datetime
import time
import pandas as pd
from platform import system as what_system
from ...config.config import hive_params, ex_data, run_hive_sql, ENV

# # windows下是无法连接hive的
# if ENV == 'WINDOWS':
#     print('ENV.ENV=WINDOWS, --> hive_connect=None')
#     hive_connect = None
# else:
#     from impala.dbapi import connect as hive_connect
#     # from ibis.impala.api import connect as hive_connect

hive_connect = None


class pyhive():
    """
    与hive有关的操作函数封装。
    """

    def __init__(self, host=None, port='10000', database='default', auth_mechanism='PLAIN', encrypt=True, queue='ai', query=None):
        self.host = host
        self.port = port
        self.database = database
        self.auth_mechanism = auth_mechanism
        self.encrypt = encrypt  # 参数是否加密
        self.queue = queue  # 队列
        self.query = query  # 如果指定query='hive'，则不需要从外部传入参数，而是直接从config读取
        self.conn = self.connect()  # 保存数据库连接

    def connect(self):
        """
        连接hive仓库。
        需要注意的是这里的auth_mechanism必须有，但database不必须
        auth_mechanism的值默认是NONE，还可以是“NOSASL”，“PLAIN”，“KERBEROS”，“LDAP”
        auth_mechanism的值取决于hive - site.xml里边的一个配置
        <name>hive.server2.authentication</name>
        <value>NOSASL</value>
        """
        # 是否直接读取配置信息
        if self.query == 'hive':
            self.host, self.port, self.database, self.auth_mechanism = hive_params
        #
        host, port, database, auth_mechanism, encrypt = self.host, self.port, self.database, self.auth_mechanism, self.encrypt
        # 如果没有传入参数，就返回None，这样即使不初始化连接也能应用pyhive中的调用存储过程函数
        if not host:
            return None
        if encrypt:
            decode = lambda string: base64.b64decode(string).decode()
            host, port, database, auth_mechanism = decode(host), decode(port), decode(database), decode(auth_mechanism)
        port = int(port)
        conn = hive_connect(host=host, port=port, database=database, auth_mechanism=auth_mechanism)
        return conn

    def ping(self):
        """如果连接断开，则重新连接"""
        pass

    def close(self):
        """关闭数据库连接"""
        self.conn.close()

    def to_log(self, string, log=None):
        """如果有日志就写日志，不然就打印"""
        log.info(string) if log else print(string)

    def execute(self, sql):
        """执行单条SQL,注意，sql不应有返回值"""
        cur = self.conn.cursor()
        cur.execute(sql)
        self.conn.commit()
        cur.close()
        # self.conn.raw_sql(sql)  # ibis

    def read_table(self, tb_name=None, sql=None, queue='ai'):
        """
        使用impyla读取数据到pandas.dataframe。
        1.设置队列
        2.获取表头
        3.每次读取50w
        4.组装成dataframe返回
        """
        if not sql:
            data_sql = "select * from {tb_name}".format(tb_name=tb_name)
        else:
            data_sql = sql
        # 先执行队列配置
        queue_sql = "set mapreduce.job.queuename={queue}".format(queue=queue)
        cur = self.conn.cursor()
        cur.execute(queue_sql)
        self.conn.commit()
        print('设置hive-mr队列为a:' + queue_sql)
        # 先取出表头
        if ' limit ' in data_sql.lower():
            tmp_data_sql = data_sql
        else:
            tmp_data_sql = data_sql + ' limit 1 '
        print('获取表的字段名：' + tmp_data_sql)
        columns_df = pd.read_sql(tmp_data_sql, self.conn)
        columns = [col.lower().split('.')[-1] for col in columns_df.columns]
        print('获取的字段名是：' + ', '.join(columns))
        # 开始取数据
        cur.execute(data_sql)
        all_data = []
        while True:
            sub_data = cur.fetchmany(500000)  # 每次取50w数据
            if len(sub_data) > 0:
                all_data.extend(list(sub_data))
                print('%s 取数- %dw-%dw' % (str(datetime.datetime.now())[:19], (len(all_data) - len(sub_data)) / 10000,
                                          len(all_data) / 10000))
            else:
                break
        # 转dataframe，添加表头
        data = pd.DataFrame(all_data, columns=columns)
        cur.close()
        print('数据量%d' % len(data))
        return data

    def read_table_using_hive_e(self, tb_name=None, sql=None, sep=',', local_path=None):
        """
        使用hive -e的方式从hive下载数据.
        原理是：hive -e "select * from tb_name limit 10">>/home/tmp.csv
        这个比用 conn.read_table 的方式快很多
        """
        # 拼接sql
        if not sql:
            data_sql = "select * from {tb_name}".format(tb_name=tb_name)
        else:
            data_sql = sql
        # 保存到本地文件地址
        sys_type = what_system().lower()
        if sys_type == 'windows':
            raise Exception('hive -e 只能在装有hive的服务器上面运行')
        # 文件地址
        if not local_path:
            file = 'tmp_hive_donwload_%s.csv' % datetime.datetime.now().strftime('%Y%m%d%H%M%S')
            file = os.path.join(ex_data, file)
        else:
            file = local_path
        # 注意cmd不要有回车换行这些东西
        sql0 = "set mapreduce.job.queuename=%s;" % self.queue
        sql1 = "set hive.cli.print.header=true;"  # 打印表头
        sql2 = "set hive.resultset.use.unique.column.names=false;"  # 字段名不带表名
        cmd = """hive -e "%s%s%s%s" | tr "\t" "%s" > %s""" % (sql0, sql1, sql2, data_sql, sep, file)
        print('-' * 100 + '\n' + cmd.replace('\n', '\\n').replace('\t', '\\t').replace('\r', '\\r') + '\n' + '-' * 100)
        # 开始
        t1 = datetime.datetime.now()
        done = os.system(cmd)
        t2 = datetime.datetime.now()
        if done > 0:
            raise Exception('下载数据失败，请检查\n%s' % cmd)
        print('完成下载数据，耗时：%d秒' % (t2 - t1).seconds)
        print('-' * 100)
        # 判断是否需要返回，如果local_path没有指定，说需要返回
        # 需要返回数据，那么就需要删除本地csv
        if not local_path:
            data = pd.read_csv(file, sep=sep)
            os.remove(file)
            return data
        # 不需要返回数据，那么返回文件地址即可
        else:
            return file

    def read_table_using_hive_e2(self, tb_name=None, sql=None, sep=',', local_path=None):
        """
        下载数据的同时，要删除字符串中特殊的字符
        要求只是简单的select语句，而不能是表关联SQL，也没有那么多的where条件.
        和read_table_using_hive_e2的区别是，要求输入的SQL更加简化，或者说在全表导出的时候更加适用
        """
        # 生成取数SQL
        data_sql = "select * from %s " % tb_name if tb_name else sql + ' '
        # 取表名
        import re
        data_sql_tmp = data_sql.replace('\n',' ')
        tb_name = re.compile(pattern=' from (.+?) ', flags=re.I).findall(data_sql_tmp)[0]
        # 取where条件
        where = ' where '+data_sql_tmp.split('where')[-1] if 'where' in data_sql_tmp else ''
        # limit条件
        limit = ' limit ' + data_sql_tmp.split(' limit ')[-1] if ' limit ' in data_sql_tmp else ''
        # 第一步，取全部字段， 判断字段类型
        if ' limit ' not in data_sql_tmp:
            columns_sql = data_sql_tmp + ' limit 1'
        else:
            columns_sql = data_sql_tmp.split(' limit ')[0] + ' limit 1'
        columns_df = pd.read_sql(columns_sql, self.conn)
        columns_df.columns = [col.lower().strip().split('.')[-1] for col in columns_df.columns]
        # 如果是字符型字段，需要增加特殊字符处理
        new_data_col = []
        for col, dtype in zip(columns_df.dtypes.index, columns_df.dtypes.values):
            if str(dtype) == 'object':
                col2 = " regexp_replace(%s, '\t|,', '' ) as %s " % (col, col)
            else:
                col2 = col
            new_data_col.append(col2)
        # 组装新的取数SQL
        new_data_col = 'select %s from %s %s %s ' % (', '.join(new_data_col), tb_name, where, limit)
        # 保存到本地文件地址
        sys_type = what_system().lower()
        if sys_type == 'windows':
            raise Exception('hive -e 只能在装有hive的服务器上面运行')
        # 文件地址
        if not local_path:
            file = 'tmp_hive_donwload_%s_%s.csv' % (tb_name, datetime.datetime.now().strftime('%Y%m%d%H%M%S'))
            file = os.path.join(ex_data, file)
        else:
            file = local_path
        # 注意cmd不要有回车换行这些东西
        sql0 = "set mapreduce.job.queuename=%s;" % self.queue
        sql1 = "set hive.cli.print.header=true;"  # 打印表头
        sql2 = "set hive.resultset.use.unique.column.names=false;"  # 字段名不带表名
        cmd = """hive -e "%s%s%s%s" | tr "\t" "%s" > %s""" % (sql0, sql1, sql2, new_data_col, sep, file)
        print('-' * 100 + '\n' + cmd.replace('\n', '\\n').replace('\t', '\\t').replace('\r', '\\r') + '\n' + '-' * 100)
        t1 = datetime.datetime.now()
        done = os.system(cmd)
        t2 = datetime.datetime.now()
        if done > 0:
            raise Exception('下载数据失败，请检查\n%s' % cmd)
        print('完成下载数据，耗时：%d秒' % (t2 - t1).seconds)
        print('-' * 100)
        # 判断是否需要返回，如果local_path没有指定，说需要返回
        # 需要返回数据，那么就需要删除本地csv
        if not local_path:
            data = pd.read_csv(file, sep=sep)
            os.remove(file)
            return data
        # 不需要返回数据，那么返回文件地址即可
        else:
            return file

    def load_table_to_csv(self, tb_name=None, sql=None, local_path=None, log=None):
        """
        废弃，请使用 read_table_using_hive_e 函数。
        从hive下载数据到本地csv。
        已经有read_table，为什么还要用这个函数呢，因为read_table读表数据很慢，因此这里用另外的方法加快读取速度。
        注意，只针对中等大小的表，比如几百万的数据量，而且可以读取到python内存中处理的表。
        另外，这里指定了，只做csv处理，其他不管.

        原理是：hive -e "select * from tb_name limit 10">>/home/tmp.csv
        :param tb_name:
        :param sql:
        :param local_path:
        :return:
        """
        """
            快递将hive表导出到csv,注意，只针对中等大小的表，比如几百万的数据量，而且可以读取到python内存中处理的表，
            另外，这里指定了，只做csv处理，其他不管.
            因此，你应该在hive中建表的时候指定分隔符就是逗号
            create table tb_name row format delimited fields terminated by ',' as select xxx from another_tb
            原理是：hive -e "select * from tb_name limit 10">>/home/tmp.csv
        """

        t1 = time.clock()
        # 如果本地已经存在，则删除
        if os.path.exists(local_path) and os.path.isdir(local_path):
            local_path = local_path + '_rename_to_this'
            self.to_log('本地已经存在的同名的文件夹，因此将需要下载的数据文件名字改为：' + local_path, log)
        if os.path.exists(local_path):
            self.to_log('本地已经存在同名的文件，将删除本地文件：' + local_path, log)
            os.remove(local_path)
        # 拼接sql
        if not sql:
            data_sql = "select * from {tb_name}".format(tb_name=tb_name)
        else:
            data_sql = sql
        # 下载
        cmd = """hive -e " {data_sql} " >>{local_path} """.format(data_sql=data_sql, local_path=local_path)
        self.to_log('执行下载语句：' + cmd, log)
        result = os.system(cmd)
        if result > 0:
            raise Exception('执行下载hive表失败：' + cmd, log)
        self.to_log('下载数据成功', log)
        # 获取表头
        data_sql_col = data_sql + ' limit 1'
        cols = pd.read_sql(data_sql_col, self.conn)
        cols = [col.split('.')[-1] for col in cols.columns]
        self.to_log('读取表头：' + str(cols))
        # 将刚才下载下来的数据转成csv
        data = pd.read_csv(local_path, sep='\t', header=None, names=cols)
        data.to_csv(local_path, index=False)
        t2 = time.clock()
        self.to_log('HIVE表保存到：%s, 数据量：%d，耗时：%f 秒' % (local_path, len(data), (t2 - t1)), log)

    def df_into_db(self, tb_name, df, datapath='', partition={}):
        """
        将数据导入hive，不同于传统数据库，目前想到的方法还挺麻烦的
        partiton：指定分区，要求是字典
        """
        pass

    def df_into_db_using_hive_e(self, tb_name, df='', file='', types='insert'):
        """
        使用hive-e的方式将数据导入到hive仓库
        如果df不为None，则将df保存为csv后导入
        如果传入的是file路径，则直接导入
        需要注意是的，你需要保证df或者csv的字段顺序和hive表保持一致
        types=overwrite, insert
        """
        # 如果传入的是dataframe
        if isinstance(df, pd.DataFrame):
            file = '%s_%s.csv' % (tb_name, datetime.datetime.now().strftime('%Y%m%d%H%M%S'))
            file = os.path.join(ex_data, file)
            df.to_csv(file, index=False, header=None)  # 注意，不需要表头
        else:
            if not os.path.exists(file):
                raise Exception('找不到指定的数据文件：' + file)
        # cmd
        cmd = """hive -e " load data local inpath '%s' %s into table %s " """ % (file, types, tb_name)
        print('将数据导入到hive：%s' % cmd)
        done = os.system(cmd)
        if done > 0:
            raise Exception('数据导入hive错误，请检查')
        return

    def new_procedure(self, file=None, params={}, log=None):
        """
        修改替换hive sql中的参数，然后方便后面shell执行存储过程。
        要求：
        这里只实现了简单的变量管理功能，不支持select xxx into @var 这种复杂的功能。
        变量的参数格式 @params_name
        修改方法：sql.replace(@params_name, value)
        注意数字和字符串的参数替换区别.

        替换参数后，删除sql中的全部注释，不知道为什么，有注释容易出错。
        """
        # 读取SQL内容
        self.to_log('即将对SQL进行参数替换，并删除所有注释 %s', log=log)
        self.to_log('参数替换是：' + str(params), log)

        if file and os.path.exists(file):
            with open(file, 'r') as f:
                sql = f.readlines()
            sql = ''.join(sql)
        else:
            raise Exception('找不到SQL文件，请检查：' + file)
        # 替换sql中的参数
        for key, value in params.items():
            sql = sql.replace(key, value)
        # 删除SQL中的注释
        pattern = r'--.+?\n'
        regex = re.compile(pattern)
        sql = regex.sub(' \n ', sql)  # 换成换行并且前后加空格防止特殊编码错误

        pattern = r'/\*.+?\*/'
        regex = re.compile(pattern)
        sql = regex.sub(' \n ', sql)  # 换成换行并且前后加空格防止特殊编码错误
        # 删除SQL前面的多个空行
        pattern = '\n{3,30}?'
        regex = re.compile(pattern)
        sql = regex.sub(' ', sql)
        # 重新将sql写到文件中，另起名字
        path, name = os.path.split(file)
        file2 = os.path.join(path, name.split('.')[0] + '_new.sql')
        self.to_log('新的存储过程是：' + file2, log=log)

        if os.path.exists(file2):
            os.remove(file2)
        with open(file2, 'w') as f:
            f.write(sql)
        return file2

    def call_procedure(self, sql_file, log=None):
        """
        使用system执行linux命令，从而执行hvie的存储过程
        """
        hive_cmd = run_hive_sql.replace('your_sql_file', sql_file)
        self.to_log('执行hive存储过程：' + hive_cmd, log)
        # 执行hive，返回0表示没有错误，非0表示有错误
        result = os.system(hive_cmd)
        if result == 0:
            return 1
        else:
            return 0

    def call_procedure2(self, sql_file, path=None, params={}, log=None):
        """
        使用system执行linux命令，从而执行hvie的存储过程.
        这个函数是将 new_procedure() 和 call_procedure() 的整合。
        """
        # 获取全路径的存储过程文件名
        if path:
            sql_file = os.path.join(path, sql_file)
        sql_file2 = self.new_procedure(file=sql_file, params=params, log=log)
        # 执行存储过程
        hive_cmd = run_hive_sql.replace('your_sql_file', sql_file2)
        self.to_log('执行hive存储过程：' + hive_cmd, log)
        # 执行hive，返回0表示没有错误，非0表示有错误
        result = os.system(hive_cmd)
        # 不管存储过程是否成功，都删除新创建的存储过程文件
        os.remove(sql_file2)
        self.to_log('删除替换参数后的SQL文件：' + sql_file2, log)
        # 返回执行结果
        if result == 0:
            self.to_log('存储过程执行成功', log)
            return 1
        else:
            self.to_log('存储过程执行失败，请跟进', log)
            return 0

    def create_new_partition(self, tb_name, partition_col, partition_value):
        """对表创建新的分区"""
        sql = """ alter table {tb_name} add partition({partition_col}='{partition_value}')""" \
            .format(tb_name=tb_name, partition_col=partition_col, partition_value=partition_value)
        cur = self.conn.cursor()
        cur.execute(sql)
        self.conn.commit()
        cur.close()

    def drop_partition(self, tb_name, partition_col, partition_value):
        """删除表分区"""
        sql = """ alter table {tb_name} drop partition({partition_col}='{partition_value}')""" \
            .format(tb_name=tb_name, partition_col=partition_col, partition_value=partition_value)
        cur = self.conn.cursor()
        cur.execute(sql)
        self.conn.commit()
        cur.close()

    def load_df_into_partition_table2(self, tb_name, df, partition_col=[], partition_value=[], local_path=None, sep=','):
        """
        上面的那个写复杂了，可以有更加简单的方法,用 load data 的方式
        你需要注意的是，df只能是一个分区的数据，因此partition_col的值都是一样的。
        导入以覆盖分区方式导入。

        df=pd.DataFrame({'id':[1,2,3],'name':['name1','name2','name3'],'dd':['d2','d2','d2']})
        ai_hive.load_df_into_partition_table2(tb_name='tmp',df=df, partition_col='dd',
        partition_value='d2')
        """
        partition_col = partition_col if isinstance(partition_col, list) else [partition_col]
        # 检查dataframe是否有多个分区
        df['tmp_partition'] = ''
        for col in partition_col:
            df[col] = df[col].apply(str)
            df['tmp_partition']=df['tmp_partition']+df[col]
        df_partitions = df['tmp_partition'].unique()
        partition_value = [str(i) for i in partition_value]
        if len(df_partitions) != 1 or df_partitions[0] != ''.join(partition_value):
            raise Exception('传进来的dataframe的分区数是:%s, 目前只能导入导入单个分区数据，请检查' % str(df_partitions))
        del df['tmp_partition']
        #
        # 获取表分区和字段名及字段顺序（顺序很重要）
        sql_p = "show partitions " + tb_name if partition_col else ''  # 注意非分区表的处理
        sql_col = "select * from %s limit 0" % tb_name
        #
        had_partition = pd.read_sql(sql_p, self.conn)
        had_partition = [p.split('=')[-1] for p in had_partition['partition']]
        cols = self.read_table(sql=sql_col)
        cols = [col.lower().split('.')[-1] for col in cols.columns]
        print('表：%s 共有 %d 个分区，字段顺序为：%s' % (tb_name, len(had_partition), str(cols)))
        #
        # 将dataframe保存到本地
        local_path = local_path if local_path else ex_data
        local_file = os.path.join(local_path, tb_name + '_' + '_'.join(partition_value) + '.csv')
        df.loc[:, cols].to_csv(local_file, index=False, header=None, sep=sep)  # 注意，不要保存表头，字段顺序要保持和表结构一致
        #
        # 导入分区的SQL
        # 普通的导入
        if len(partition_col) == 0:
            load_hive_sql = """hive -e "load data local inpath '{local_file}' overwrite into table {tb_name}
                    " """.format(local_file=local_file, tb_name=tb_name)
        # 只有一个分区字段的导入
        if len(partition_col) == 1:
            load_hive_sql = """hive -e "load data local inpath '{local_file}' overwrite into table {tb_name} 
                    partition({partition} = '{value}')" """.format(local_file=local_file, tb_name=tb_name,
                                                                   partition=partition_col[0], value=partition_value[0])
        # 有2个分区字段的导入
        if len(partition_col) == 2:
            load_hive_sql = """hive -e "load data local inpath '{local_file}' overwrite into table {tb_name} 
                            partition({partition1} = '{value1}', {partition2} = '{value2}')" """.format(
                local_file=local_file, tb_name=tb_name, partition1=partition_col[0], value1=partition_value[0],
                partition2=partition_col[1], value2=partition_value[1])
        # 指定导入语句
        print('%s\n%s\n%s'%('-'*200, load_hive_sql, '-'*200))
        code = os.system(load_hive_sql)
        # 删除临时文件
        os.remove(local_file)
        if code == 0:
            print('将数据导入到表%s：%s' % (tb_name, load_hive_sql))
            return 1, '0'
        else:
            error = '错误发生在：os.system 将数据导入到表%s：%s' % (tb_name, load_hive_sql)
            return 0, error



def test():
    # 测试下载数据到csv
    from .config.config import hive_params
    from .common.database.pyhive import pyhive
    host, port, database, auth_mechanism = hive_params
    ai_hive = pyhive(host, port, database, auth_mechanism)
    ai_hive.load_table_to_csv(tb_name='predict',
                              local_path='/home/predict.csv')
