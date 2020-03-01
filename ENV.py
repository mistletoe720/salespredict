# -*- coding: utf-8 -*-
base_info = """
----------------------------------------------------------------------------------------------------------------------------
    这个ENV.py指明当前环境是开发环境，测试环境，还是正式生成环境，config中根据环境变量
    动态加载对应的参数，这样防止将参数写死而导致错误或者测试事故。
    
    你需要在 ./config/config.py 中配置
        prod_path               正式环境下的代码目录
        prod_log_path           正式环境下的日志目录，可以不用配置，因为已经写日志服务器
        prod_exchange_data_path 正式环境下的临时数据目录
    
    注意，在项目根目录的 ENV.py 中指定 '生产环境' 还是 '测试环境'，
    注意，git 中不要推送这个 ENV.py 文件
    每次检查.
    ENV='LINUX'     本地linux开发环境
    ENV='WINDOWS'   本地windows开发环境
    ENV='TEST'      测试组测试环境
    ENV='PROD'      正式生产环境
    ----------------------------------------
    | 这里 ENV = {ENV} 
    ----------------------------------------
    日志目录      log_path = {LOG_PATH}
    临时数据路径  exchange_data_path = {DATA_PATH}
----------------------------------------------------------------------------------------------------------------------------
"""
# 第一步，指定环境（你只需要定义环境，其他不用管）
ENV = 'WINDOWS'  # *******************************************************************
# ENV = 'PROD'   # 线上环境

# 第二步，从 config 中读取正式环境的目录
from .config.config import prod_path, prod_log_path, prod_exchange_data_path

# 第三步，获取当前运行环境的目录
from os import path
from platform import system

current_path = path.split(path.abspath(__file__))[0]

# 第四步，校验
if system().lower() == 'windows' and ENV == 'PROD':
    raise Exception('当前系统是windows，但是ENV=PROD，请检查')

if current_path != prod_path and ENV == 'PROD':
    info = """
    ENV=PROD，但是路径不对，请核查...
    当前目录是：%s
    正式环境目录是：%s
    """ % (current_path, prod_path)
    raise Exception(info)

if current_path == prod_path and ENV != 'PROD':
    info = """
    当前路径是生产环境路径，但是ENV不对，请核查...
    当前 ENV=%s
    而正式环境 ENV=PROD
    """ % (ENV)
    raise Exception(info)

# 第5步，如果 ENV <> PROD，则需要生成开发测试需要的目录
tmp_path = path.abspath(path.join(path.dirname(__file__), "../.."))
log_path = path.join(tmp_path, 'data_tmp', 'log_path')
exchange_data_path = path.join(tmp_path, 'data_tmp', 'data_path')

# 第六步，打印一次信息
if ENV=='PROD':
    print(base_info.format(ENV=ENV, LOG_PATH=prod_log_path, DATA_PATH=prod_exchange_data_path))
else:
    print(base_info.format(ENV=ENV, LOG_PATH=log_path, DATA_PATH=exchange_data_path))
