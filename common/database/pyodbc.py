# -*- coding: utf-8 -*-
"""
使用ODBC与数据库通信。
注意：需要事先在系统中配置好ODBC数据源
"""

import pypyodbc as pyodbc
import base64
import traceback

def connect(dsn,UID='',PWD=''):
    """使用odbc数据源连接，需要指定dsn"""
    # 有时候除了dsn,还需要输入账号密码的
    decode = lambda string: base64.b64decode(string).decode()
    if UID and PWD:
        UID,PWD = decode(UID),decode(PWD),
        string = "DSN=%s;UID=%s;PWD=%s" %(dsn,UID,PWD)
        return pyodbc.connect(string)
    if UID and not PWD:
        UID = decode(UID)
        string = "DSN=%s;UID=%s" %(dsn,UID)
        return pyodbc.connect(string)
    if not UID and not PWD:
        string = "DSN=%s" %dsn
        return pyodbc.connect(string)



def df_into_sybase(conn, df, tb_name):
    """将df批量导入数据库"""
    # 如果是空的数据框，什么都不做
    if len(df) == 0:
        return None

    cursor = conn.cursor()
    # 先将df全部转成字符串格式，主要是针对数值，df中的时间此时肯定是字符串类型
    df2 = df.applymap(lambda s: str(s))
    # 处理空值问题，因为刚才转成字符串时，None被转成"None"
    nan_df = df.notnull()
    df2 = df2.where(nan_df, '')  # 对于原来是NaN的，要转成空字符串

    # 创建导数的SQL
    sql = "insert into table_name ( columns ) values ( num_? )"
    cols = list(df2.columns)
    columns = ','.join(cols)
    value_str = ["?" for i in range(len(cols))]
    value_str = ",".join(value_str)  # 得到'?,?,?...'的字符串
    sql = sql.replace('table_name', tb_name)
    sql = sql.replace('columns', columns)
    sql = sql.replace('num_?', value_str)
    # 将df转成list-tuple格式，才能导入数据库
    param = df2.to_records(index=False).tolist()
    # 入库,每次提交1w
    try:
        cnts = len(param)
        for i in range(0,len(param),100000):
            param2 = param[i:i + 100000]
            cursor.executemany(sql, param2)
            conn.commit()
            print('data of %.2fw - %.2fw /%d into %s' % (i / 10000, (i + 10000) / 10000, cnts, tb_name))
    except:
        traceback.print_exc()
        conn.commit()
        cursor.close()
        raise Exception('有错误')


def df_into_db(conn,table,df,types='insert'):
    """使用pypyodbc将df导入数据库"""
    # 判断是否需要清空表
    cursor = conn.cursor()
    if types.upper() in "DELETE TRUNCATE":
        cursor.execute("delete from " + table)
        # cursor.execute("commit")  # sybaseIQ需要commit，oracle也要，但是SqlServer和MySQL不需要
    # 将df转成字符形式，填充缺失值
    # 处理空值问题，因为刚才转成字符串时，None被转成"None"
    df2 = df.applymap(lambda s: str(s))
    nan_df = df.notnull()
    df2 = df2.where(nan_df, '')  # 对于原来是NaN的，要转成空字符串
    # 创建导数的sql
    # 创建导数的SQL
    sql = "insert into table_name ( columns ) values ( num_? )"
    cols = list(df2.columns)
    columns = ','.join(cols)
    value_str = ["?" for i in range(len(cols))]
    value_str = ",".join(value_str)  # 得到'?,?,?...'的字符串
    sql = sql.replace('table_name', table)
    sql = sql.replace('columns', columns)
    sql = sql.replace('num_?', value_str)
    # 将df转成list-tuple格式，才能导入数据库
    param = df2.to_records(index=False).tolist()
    # 批量入库，每次1w
    for i in range(0,len(param),10000):
        param2 = param[i:i+10000]
        cursor.executemany(sql, param2)
        conn.commit()
        print('data of %d-%d into db'%(i,i+10000))
    cursor.close()










def main():
    pass

if __name__ == '__main__':
    main()
