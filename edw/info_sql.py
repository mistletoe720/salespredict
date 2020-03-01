# -*- coding: utf-8 -*-
from pyspark.sql import functions as F
from pyspark.sql import SparkSession
import datetime

# 使用spark生成服装产品基础信息表
def main():
    t1 = datetime.datetime.now()

    # 创建spark对象
    spark = SparkSession\
        .builder\
        .enableHiveSupport()\
        .getOrCreate()

    # 自定义函数
    def is_nulludf(fieldValue, defaultValue):
        if fieldValue == None:
            return defaultValue
        return fieldValue

    spark.udf.register("is_nulludf", is_nulludf)


    # 生成服装产品基础信息表
    # 1.创建基表
    jdbcDF = spark.sql("""
        select 
          from 

    """)
    jdbcDF.createOrReplaceTempView("tmp_basic")

    # 写入hive数据库
    # spark.sql("drop table if exists ")
    jdbcDF.write.saveAsTable("", None, "overwrite", None)
    print(jdbcDF.dtypes)

    # 保存到mysql当前表
    # jdbcDF.write.mode("overwrite").format("jdbc").options(
    #     url='jdbc:mysql://',
    #     user='',
    #     password='',
    #     dbtable="",
    #     batchsize="20000",
    # ).save()

    # 关闭spark
    spark.stop()

    t2 = datetime.datetime.now()
    print('\n' + '-' * 100)
    print('开始时间：' + str(t1))
    print('结束时间：' + str(t2))
    print('-' * 100 + '\n')


if __name__ == '__main__':
    main()
