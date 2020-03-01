# -*- coding: utf-8 -*-
"""
在这里写文档说明...
"""
import pandas as pd
from .config.config import ex_data
from .common.database import pyfile
from .common.parallel import parallel_func

# -------------------------------------------------------------------------------------------------
# 第一步：定义函数

# 定义需要并行计算的函数，注意输入到参数和顺序
# 第一个参数必须是 infile，是一个 pickle 文件，函数读取该数据文件
# 第二个参数必须是 outfile，是一个 pickle 文件，函数的结果保存到该文件，结果必须是dataframe
# 第3,4,5...个参数看情形而定
def my_func(infile, outfile, statedate, now):
    """注意args是list，里面的元素是各个参数，而且需要注意参数的顺序"""
    pypickle = pyfile.pypickle()
    data = pypickle.read(infile)  # 第一步，读取数据
    data['aa'] = str(statedate) + str(now)  # 第二步，计算
    pypickle.write(outfile, data)  # 第三步，将计算结果写到文件，只能是 dataframe
    return outfile

# -------------------------------------------------------------------------------------------------
# 第二步，函数封装
# 紧接着，对刚才的函数做一层封装，固定写法，照抄就好了
def parallel_func(args):
    return my_func(*args)

# -------------------------------------------------------------------------------------------------
# 第三步，并行计算

def _test():
    # 生成测试数据
    import numpy as np
    df = pd.DataFrame(np.random.rand(5, 5), columns=['a', 'b', 'c', 'd', 'e']). \
        applymap(lambda x: int(x * 10))
    #
    # 并行计算测试的参数
    func_params = ['2018-09-10', '12:12:12']  # 参数的顺序要和 my_func 的参数除了 infile,outfile 之外保持顺序一致
    id_col = ['a', 'b']                       # 分组字段，相当于每条记录的id
    partition_cnt = 2                         # 要分几个子文件
    n_cpu = 2                                 # 并行计算cpu数量，一般和partition_cnt相同，可以为None
    path = ex_data                            # 临时数据保存路径，可以使用默认值
    prefix = 'test'                           # 文件前缀，一般是项目名称或者子过程名称，可以只用默认值
    #
    # 开始计算，结果返回一个dataframe
    # 或者dataframe写pickle
    data_df = parallel_func.parallel(parallel_func,
                       func_params=func_params,
                       df=df,
                       id_col=id_col,
                       partition_cnt=partition_cnt,
                       n_cpu=n_cpu,
                       path=path,
                       prefix=prefix)
    print(data_df)
    #
    # 当然，也可以经结果写pickle
    result_file = r'd:\result_data.pickle'              # 如果指定result_file，则将结果写到pickle文件中
    file = parallel_func.parallel(parallel_func,
                       func_params=func_params,
                       df=df,
                       id_col=id_col,
                       partition_cnt=partition_cnt,
                       n_cpu=n_cpu,
                       path=path,
                       prefix=prefix,
                       result_file=result_file)
    print(file)


