# -*- coding: utf-8 -*-
"""

定义那些表需要备份，备份频率等信息




config：


{ 'source_tb': 'tb_name1',        MySQL源表
  'target_tb': 'tb_name2',        hive目标表
  'partition_col': ['statedate']  注意把日期字段放在第一个位置，如果有多个分区字段的话
  'frequence': 1,2,3,4            表示每1(2,3,4)天备份，1表示每天备份，2表示每2天备份1次，3表示每3天备份1次
  'job_hour': [5, 12, 17]         表示5点，12点，17点的时候备份，当一天多次备份时候有用，多天一次相当于指定什么时候启动任务备份
}
"""

import os
from .config.config import ex_data as root_ex_data
from .business.data_backup import __path__

backup_tb = [
    {'source_tb': '',
     'target_tb': '',
     'partition_col': ['statedate'],  # 注意把日期字段放在第一个位置
     'frequency': 1,
     'job_hour': [8, 14],
     'describe': '预测结果备份到hive'
     }

]

# 存放上次备选信息的json文件
last_backup_info_file = os.path.join(__path__[0], 'table_last_backup_time.json')
# 临时数据文件路径
local_path = root_ex_data
