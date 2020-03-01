"""
创建 pandas-->hive 的数据结构映射关系

注意，这里定义方法并不是很普遍，应为硬性规定了一些数据类型，只适用于不那么特殊的场景下使用。


根据 pandas dataframe 的结构，创建对应的 hive 表，在备份MySQL，转存数据到hive的时候很实用。

注意，这里规定，只是用3中数据结构，int，double，string
至于时间，请转存string存储。

至于，dataframe 导入 hive，则使用 pyhive.load_df_into_partition_table2 方法即可


"""
import pandas as pd


def df_map_hive_schema(df):
    """根据df的dtypes转换到hive的数据类型"""
    # col='name'
    # col='id'
    # col='date'
    hive_schema = df.dtypes.copy()
    for col in df.columns:
        if 'object' in str(hive_schema[col]):
            hive_schema[col] = 'string'
        elif 'int' in str(hive_schema[col]):
            hive_schema[col] = 'int'
        elif 'float' in str(hive_schema[col]):
            hive_schema[col] = 'float'
        elif 'datetime' in str(hive_schema[col]):
            hive_schema[col] = 'string'
            print('列 %s 是日期类型，将使用string类型存储' % col)
        else:
            print('列 %s 是 %s 类型，将使用string类型存储' % (col, str(hive_schema[col])))
            hive_schema[col] = 'string'
    return hive_schema


def create_hive_table_sql(tb_name, hive_schema, sep=',', partition_col=[]):
    """创建hive表SQL
    hive_schema是pandas.series
    partition_col是分区字段，必须是string类型
    sep：hdfs文件的列分隔符
    sep=','
    tb_name='tb_name'
    partition_col=['date']
    partition_col=[]
    """
    # 将分区字段从主体字段中删除
    if partition_col:
        for col in partition_col:
            hive_schema = hive_schema.drop(labels=col)
    # 主体字段的sql
    cols_sql = ',\n '.join(['%s %s' % (col, dtype) for col, dtype in hive_schema.items()])
    # 分区字段的sql
    if partition_col:
        par_sql = 'partitioned by ( %s ) ' % (', '.join(['%s string' % col for col in partition_col]))
    else:
        par_sql = ''
    # 总体SQL
    create_sql1 = "drop table if exists %s" % tb_name
    create_sql2 = """
    create table {tb_name} ( \n {cols_sql} ) \n {partition_sql} \n ROW FORMAT DELIMITED FIELDS TERMINATED BY '{sep}'
    """.format(tb_name=tb_name,
               cols_sql=cols_sql,
               partition_sql=par_sql,
               sep=sep)
    return create_sql1, create_sql2


def _test():
    # 创建测试dataframe
    df = pd.DataFrame({'id': [1, 2, 3, 4, 5],
                       'name': ['a', 'b', 'c', 'd', 'e'],
                       'score': [17.5, 22.3, 45, 98, 86.5],
                       'date': pd.date_range(start='2018-10-01', periods=5)})
    # 获取映射的hive表数据类型
    hive_schema = df_map_hive_schema(df)
    # 创建对应的建表SQL
    sql1, sql2 = create_hive_table_sql(tb_name='test_tb', hive_schema=hive_schema, sep=',', partition_col=[])
    print(sql1)
    print(sql2)
    #
    # df导入hive表
