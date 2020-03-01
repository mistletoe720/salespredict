# -*- coding: utf-8 -*-
"""
这是整个代码的全局配置文件，用于一些固定的系统交互和参数设定.

注意，这里将不同环境的参数写在一起，而不是分开多个配置文件，因为可能会忘记维护多个配置文件啊.
"""
# 正式生成环境目录，写死，不可动态生成
# 新环境
prod_path = ''
prod_log_path = ''
prod_exchange_data_path = ''

from .. import ENV  # 注意，要先定义上面的正式环境目录，才能执行这句import ENV

# 日志目录,如果有指定则用指定的，没有则用默认的
# 只有在正式环境中，才会启动默认的

if ENV.ENV == 'PROD':
    default_log_path = prod_log_path
    log_path = default_log_path
else:
    log_path = ENV.log_path

# 临时数据目录,如果有指定则用指定的，没有则用默认的
if ENV.ENV == 'PROD':
    default_data_path = prod_exchange_data_path
    ex_data = default_data_path
else:
    ex_data = ENV.exchange_data_path
