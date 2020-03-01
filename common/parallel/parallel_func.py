# -*- coding: utf-8 -*-
"""
并行计算，特别适合特征的并行计算，需要注意的是，在计算函数中，不要出现print, log等打印操作，不然你会不知道到底是哪个进行打印的

基本约束：
需要并行的计算函数 func 中，其基本逻辑是，传入
func(input_file, output_file, other_params):
    input_file:
    output_file:
    other_params:

func 的处理流程必须是：
    1、通过 input_file 读取数据（pickle的dataframe）
    2、计算
    3、将 结果dataframe写到 output_file （pickle的dataframe）

具体的使用方法，参见 test 函数

"""
import pandas as pd
from multiprocessing import Pool
import datetime
import os
from ...config.config import ex_data
from ..database import pyfile
from . import data_partition

pyos = pyfile.pyos()
pypickle = pyfile.pypickle()



def old_data(path, prefix):
    """删除3天之前的数据，如果是相同的任务，删除之前的数据"""
    # 删除3天前的数据
    last = 'parallel_'+(datetime.datetime.now()-datetime.timedelta(days=3)).strftime('%Y%m%d_%H%M%S')
    for file in os.listdir(path):
        if file.startswith('parallel_2018') and file<=last:
            os.remove(os.path.join(path, file))
    # 相同的项目，删除1天前的数据
    last = 'parallel_%s_%s_data_split_'%((datetime.datetime.now()-datetime.timedelta(days=1)).
                                         strftime('%Y%m%d_%H%M%S'), prefix )
    for file in os.listdir(path):
        if file.startswith('parallel_2018') and file<=last:
            os.remove(os.path.join(path, file))


def parallel(func, func_params=None, df=None, id_col=[], partition_cnt=1, n_cpu=None,
             path=None, prefix='', result_file=''):
    """
    对df进行分割分组，然后对每个分组应用相同的func函数
    :param func: 需要运算的函数
    :param func_params: func的参数，一般是
    :param df: 整体数据集
    :param id_col: 分区字段
    :param partition_cnt: 需要分区的数量
    :param n_cpu: 并行cpu数量
    :param path: 临时数据目录
    :param prefix: 临时数据文件的前缀
    :param result_file: 将结果保存到文件并返回文件名，否则返回并行计算的结果
    :return:
    """
    t1=datetime.datetime.now()
    # 本批次处理
    batch='parallel_%s_'%datetime.datetime.now().strftime('%Y%m%d_%H%M%S')
    # 数据切分的文件前缀
    split_prefix = batch+'%s_data_split_'%prefix
    print('本地并行计算的数据切分数据子集文件前缀是：%s' % split_prefix)
    # 每个子集处理结果的前缀
    result_prefix = batch + '%s_data_result_'%prefix
    print('本地并行计算的子集处理结果文件前缀是：%s' % result_prefix)
    # 判断临时数据目录是否存在
    if not path:
        path=os.path.join(ex_data, 'parallel_run_path')
    pyos.mkdir_if_not_exists(path)
    print('本地并行计算的数据目录是：%s'%path)
    # 先删除旧数据
    old_data(path, prefix)
    # -------------------------------------------------------------------------------------------------------
    # 第一步，分割数据集
    id_col = id_col if isinstance(id_col, list) else [id_col]   # 分割数据集的id字段，相同的ID数据分在同一个子集中
    t1=datetime.datetime.now()
    all_sub_dat_file = data_partition.df_partition(df, id_col=id_col, path=path, prefix=split_prefix, par_cnt=partition_cnt, to_pickle=True)
    t2=datetime.datetime.now()
    print('数据切分耗时：%d秒，切分成 %d 个子文件'%((t2-t1).seconds, len(all_sub_dat_file)))
    # -------------------------------------------------------------------------------------------------------
    # 生成每个子集处理后的结果文件名称
    all_result_file=[]
    for sub_file in all_sub_dat_file:
        all_result_file.append(sub_file.replace(split_prefix, result_prefix))
    print('生成每个子集处理后的结果文件名称')
    # -------------------------------------------------------------------------------------------------------
    # 重新包装func
    # def p_func(args):
    #     return func(*args)
    # 组装func的参数，注意顺序
    func_params = func_params if isinstance(func_params, list) else [func_params]  # 转成list类型
    all_params=[]
    for i in range(len(all_sub_dat_file)):
        sub_param=[]
        sub_param.append(all_sub_dat_file[i])   # 第一个是 input_file
        sub_param.append(all_result_file[i])    # 第二个是 output_file
        sub_param.extend(func_params)           # 第三个到最后一个是 其他参数
        all_params.append(sub_param)
    # -------------------------------------------------------------------------------------------------------
    # 并行计算
    n_cpu = n_cpu if n_cpu else partition_cnt
    pool = Pool(n_cpu)
    parallel_result_file = pool.map(func, iterable=all_params)
    pool.close()
    pool.join()
    print('完成所有子数据集的并行计算')
    # -------------------------------------------------------------------------------------------------------
    # 读取并行计算子集的结果，组装成一个大的dataframe
    all_result_data = pd.DataFrame()
    for sub_result_file in parallel_result_file:
        sub_data = pypickle.read(sub_result_file)
        all_result_data = pd.concat([all_result_data, sub_data])
    print('读取并行计算子集的结果，组装成一个大的dataframe')
    # 结果是否写文件
    if result_file:
        pypickle.write(result_file, all_result_data)
        print('结果写到文件：%s'%result_file)
    # -------------------------------------------------------------------------------------------------------
    # 删除临时文件
    pyos.remove_files_if_exists(path, file_prefix=split_prefix)
    pyos.remove_files_if_exists(path, file_prefix=result_prefix)
    print('删除临时数据文件')
    t2 = datetime.datetime.now()
    print('完成全部并行计算，耗时：%d 秒'%(t2-t1).seconds)
    # 返回
    if result_file:
        return result_file
    else:
        return all_result_data


def _test():
    print('使用案例，参考同级目录的另一个文件 parallel_func_demo.py，格式要求还是有点麻烦的')
