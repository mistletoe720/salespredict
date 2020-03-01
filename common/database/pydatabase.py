# -*- coding: utf-8 -*-
"""
数据库基类
"""
import traceback
import pandas as pd
import datetime

class database():
    """数据库基类，其他MySQL，hive等数据库操作类集成基类，并实现基类的方法"""
    def __init__(self,conn):
        self.conn=conn

    def connect(self):
        """连接数据库"""
        pass

    def ping(self):
        """判断数据库是否连接断开，如果断开，则重新连接"""
        # 下面是MySQL的写法
        self.conn.ping()

    def reconnect(self):
        """重新连接数据库"""
        pass

    def execute(self,sql):
        """执行SQL"""
        self.ping()
        try:
            cur=self.conn.cursor()
            cur.execute(sql)
            self.conn.commit()
            cur.close()
            return 1,''
        except Exception as e:
            self.conn.rollback()
            error=traceback.format_exc()
            print(error)
            return 0,error

    def close(self):
        """关闭数据库连接"""
        self.conn.close()

    def read_table(self,tb_name=None,sql=None):
        """读取表数据"""
        if not sql:
            sql="select * from {tb_name}".format(tb_name)
        data=pd.read_sql(sql, self.conn)
        data.columns=[col.lower().split('.')[-1] for col in data.columns]
        return data

    def dict_into_db(self,tb_name,data):
        """
        将字典类型的数据导入数据库。这个在参数服务器，日志表等方面很有用。
        需要注意的是，这里使用MySQL的写法，具体数据库需要另外实现改方法。
        """
        # data={'id': 1, 'name': 'myname', 'score':98.5}
        cols = ','.join([k for k, v in data.items()])
        values = "','".join([str(v) for k, v in data.items()])
        values = "'" + values + "'"
        sql = "insert into {tb} ({cols}) values ({values})".format(tb=tb_name, cols=cols, values=values)
        self.execute(sql)

    def write_log(self,tb_name,data):
        """将dict类型的data写到日志表.功能和dict_into_db一样，只是为了方便识别而已"""
        self.dict_into_db(tb_name,data)

    def get_param(self, pkey, tb='', sql=None):
        """参数服务器：从数据库读取参数值"""
        # 如果没有传入sql，就从指定的表中读取参数
        if not sql:
            sql="select pvalue from {tb} where pkey='{pkey}'".format(tb=tb,pkey=pkey)
        try:
            param=pd.read_sql(sql, self.conn)
            return param['pvalue'].iat[0]
        except Exception as e:
            print('参数 pkey=%s 不存在'%pkey)
            return None

    def update_param(self, conn, pkey, pvalue, tb_name=''):
        """参数服务器：更新参数。首先要确保参数是存在的，不然就插入。需要对pkey建立索引。"""
        # 判断是否存在
        sql="select 1 from {tb} where pkey='{pkey}'".format(tb=tb_name,pkey=pkey)
        param=pd.read_sql(sql,conn)
        # 如果没有，则插入
        updatetime=str(datetime.datetime.now())[:19]
        if len(param)==0:
            sql="insert into {tb} (pkey, pvalue) values ('{pkey}','{pvalue}')".format(tb=tb_name,pkey=pkey,pvalue=pvalue)
        else:
            sql="update {tb} set pvalue='{pvalue}', updatetime='{updatetime}' where pkey='{pkey}' ".\
                format(tb=tb_name, pkey=pkey, pvalue=pvalue, updatetime=updatetime)
        # 执行
        state, error=self.execute(sql)
        return state

    def df_into_db(self, tb_name, df, types='insert'):
        """
        将df导入mysql数据库，不需要特别处理日期列，相比oracle还是很方便的.
        注意，这里是以MySQL写的，如果是其他数据库，需要重新实现该方法。
        """
        if len(df) == 0:
            return None
        # 先将df全部转成字符串格式，主要是针对数值，df中的时间此时肯定是字符串类型
        df2 = df.applymap(lambda s: str(s))
        # 处理空值问题，因为刚才转成字符串时，None被转成"None"
        nan_df = df.notnull()
        df2 = df2.where(nan_df, None)  # 对于原来是NaN的，要转成空字符串
        #
        self.conn.ping()
        cur = self.conn.cursor()
        # 判断是否需要清空表
        if types.upper() in "TRUNCATE,DELETE":
            truncate_sql = "delete from " + tb_name
            cur.execute(truncate_sql)
            self.conn.commit()

        # 将df转成list-tuple格式，才能导入数据库
        param = df2.to_records(index=False).tolist()

        # 创建入库的sql
        sql = "insert into tb_name (cols) values (%s_many_times) "
        cols = list(df2.columns)
        cols_string = ', '.join(cols)
        num = len(list(df2.columns))
        num_string = ','.join(['%s' for i in range(num)])

        sql = sql.replace('tb_name', tb_name)
        sql = sql.replace('cols', cols_string)
        sql = sql.replace('%s_many_times', num_string)
        # 入库,每次提交1w
        try:
            cnts = len(param)
            for i in range(0, len(param), 10000):
                param2 = param[i:i + 10000]
                cur.executemany(sql, param2)
                self.conn.commit()
                print('data of %.2fw - %.2fw /%d into %s' % (i / 10000, (i + 10000) / 10000, cnts, tb_name))
            cur.close()
            return None
        except:
            error = traceback.format_exc()
            self.conn.commit()
            cur.close()
            raise Exception(error)

    def delete_old_data(self, tb_name, value, col='statedate',
                        keepdays=None, dateformat='%Y-%m-%d'):
        """
        删除表中超过多少天的数据或者或者指定统计日期当天的数据。
        这里有个基本假设，就是日期的字段名叫statedate,日期都是字符串，格式都是%Y-%m-%d
        如果 keepday>0，表示要删除以往的数据
        如果 keepday=0或者None，表示删除指定日期当天数据
        """
        # 判断是否是删除今天的数据
        # keepdays=None,删除今天的数据
        if not keepdays:
            sql="delete from {tb} where {col}='{value_}'".format(tb=tb_name,col=col,value_=value)
        else:
            lastday = (pd.to_datetime(value) - datetime.timedelta(keepdays)).strftime(dateformat)
            sql = "delete from {tb} where {col}<='{value_}'".format(tb=tb_name, col=col, value_=lastday)
        # 执行
        state,error=self.execute(sql)
        return state

    def back_old_data(self,source_tb,target_tb, startday=None, endday=None, col='statedate'):
        """将当前表数据到历史表"""
        sql="insert into {target_tb} select * from {source_tb} " \
            "where {col}>='{startday}' and {col}<='{endday}'".\
            format(target_tb=target_tb,source_tb=source_tb,col=col,startday=startday,endday=endday)
        #
        state,error=self.execute(sql)
        return state
