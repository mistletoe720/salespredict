# -*- coding: utf-8 -*-
import os
from .common.log.log import logger
from .edw.info import config_info as config_rpt
from .common.run_job import run_job
from .common.database import hive2mysql


# 使用spark生成信息表
def main():
    # 导入日志
    log = logger('信息表.log', logname='info')
    log.info('执行SPARK存储过程，生成信息表')

    # -----------------------------------------------------------------------------------------
    # 在config中定义spark_job的参数
    # 执行spark任务，错误则直接报错
    sql_file = 'info_sql.py'
    path = os.path.split(os.path.abspath(__file__))[0]
    pyfile = os.path.join(path, sql_file)
    params = r""" --driver-memory 20g --num-executors 12 --executor-cores 5 --executor-memory 5G  """
    need = 30
    job_name = 'info_sql.py'
    timeout = 30 * 60
    run_job.run_spark(pyfile, params, need, job_name, timeout, endtime=None, retry=3)
    # -----------------------------------------------------------------------------------------
    # 在config中定义需要更新哪些参数
    # 更新参数表
    log.info('更新参数表')
    params = config_rpt.etl_1_result_tb
    run_job.update_params_tb_done(params, log)

    # -----------------------------------------------------------------------------------------
    # 数据保存一份到mysql
    h2m = hive2mysql.hive2mysql(mysql_query='', hive_query='')
    h2m.hive_data_to_mysql(mysql_tb_name='', hive_tb_name='', truncate_mysql_tb=True)
    log.info('数据保存一份到mysql，完成')



if __name__ == '__main__':
    main()

