# -*- coding: utf-8 -*-
"""
将csv文件按照某一个特殊字典进行分割成多个文件，用于大文件的分割。
"""

import pandas as pd
import os
import pickle
from numpy import linspace


def file_partition(file, cnt_perfile, id_col, prefix):
    '''
    读取文件，并且将文件按照指定字段进行分割成小文件。用于大文件的分割，后边用于并行计算。
    每个对象有独立ID，每个对象有多行，但是某些字段值是不一样的，比如一个sku有多天的数据记录。
    注意，默认file是在数据库中按照ID字段进行排序后的。
    :param file: 数据文件
    :param cnt_perfile:每个子文件的id数量
    :param id_col: 分组依据，也就是每个对象的id
    :param prefix: 保存子文件的前缀
    :return:
    '''
    all_files = []
    # 获取列名
    with open(file, 'r') as f:
        columns = f.readline()[:-1].split(',')
    tmp_data = pd.DataFrame(columns=columns)
    # 初始化
    id_cnt = 0
    file_id = 1
    file_path = os.path.split(file)[0]
    subfile_name = os.path.join(file_path, prefix + '_%03d.csv' % file_id)
    tmp_data.to_csv(subfile_name, index=False)  # 写入列名，后面追加写入时不需要写，否则文件没列名很难受的
    all_files.append(subfile_name)
    # 开始读取数据
    reader = pd.read_csv(file, chunksize=10000)
    for data in reader:
        cnt = len(data[id_col].unique())  # 这批数据里面有多少id
        id_cnt += cnt  # 累积了多少id
        # 如果id数量还不够，直接写到文件
        if id_cnt < cnt_perfile:
            data.to_csv(subfile_name, index=False, header=None, mode='a')
            continue
        # 如果刚好够了，除了写到文件，还要创建下一个数据文件
        if id_cnt == cnt_perfile:
            data.to_csv(subfile_name, index=False, header=None, mode='a')
            id_cnt = 0
            file_id += 1
            subfile_name = os.path.join(file_path, prefix + '_%03d.csv' % file_id)
            tmp_data.to_csv(subfile_name, index=False)
            all_files.append(subfile_name)
            continue
        # 如果超出了指定数量，那么就需要分割成两个子文件
        else:
            ids = []
            for id in data[id_col]:
                if id in ids:
                    continue
                else:
                    ids.append(id)
                if len(ids) + id_cnt < cnt_perfile:
                    continue
                else:
                    break
            # 取出属于前1个file的data
            subdata = data.loc[data[id_col].isin(ids)]
            subdata.to_csv(subfile_name, index=False, header=None, mode='a')
            # 构造下一个file
            id_cnt = 0
            file_id += 1
            subfile_name = os.path.join(file_path, prefix + '_%03d.csv' % file_id)
            tmp_data.to_csv(subfile_name, index=False)
            all_files.append(subfile_name)
            # 剩余数据写到这里
            subdata2 = data.loc[-data[id_col].isin(ids)]
            subdata2.to_csv(subfile_name, index=False, header=None, mode='a')
    # 返回
    return all_files


def test():
    # 生成数据文件
    import numpy as np
    data = np.random.random((20, 3)) * 10
    data = pd.DataFrame(data, columns=['a', 'b', 'c'])
    data['id'] = range(len(data))
    data = data.applymap(int)
    file = r"C:\Users\suzhenyu\Desktop\a.csv"
    data.to_csv(file, index=False)
    # 分割数据集
    file_partition(file, cnt_perfile=4, id_col='id', prefix='test_split')


def df_partition(df, cnt_perfile=None, id_col=None, path=None, prefix='prefix', par_cnt=10, to_pickle=False):
    '''
    上面的一个函数是将file读取进来后分割，这里是对传进来的dataframe进行分割。
    将dataframe按照指定id_col字段进行分割成小文件。用于大文件的分割，后边用于并行计算。
    每个对象有独立ID，每个对象有多行，但是某些字段值是不一样的，比如一个sku有多天的数据记录。
    注意，默认file是在数据库中按照ID字段进行排序后的。
    :param df: 数据文件
    :param cnt_perfile:每个子文件的id数量(已废弃)
    :param id_col: 分组依据，也就是每个对象的id
    :param path: 存放的目录
    :param prefix: 保存子文件的前缀
    :param par_cnt: 将df分割成多少个子文件，最多不超过100个
    :param par_cnt: to_pickle 结果是否保存到pickle，更快
    :return:
    '''
    # par_cnt = 15
    # df=pd.DataFrame(pd.Series([str(i) for i in range(11)]),columns=['a'])
    # id_col='a'
    # all_par=list(range(100))
    if isinstance(id_col, str):
        df['hash_col'] = df[id_col].apply(lambda x: int(str(hash(x))[-2:]))  # 转哈希后取后两位数字用于分区
    else:
        df['tmp_hash_col'] = ''
        for col in id_col:
            df['tmp_hash_col'] = df['tmp_hash_col'] + df[col].apply(str)
        df['hash_col'] = df['tmp_hash_col'].apply(lambda x: int(str(hash(x))[-3:]))  # 转哈希后取后两位数字用于分区
        del df['tmp_hash_col']
    # 计算分区数量
    all_par = df['hash_col'].unique()
    all_par.sort()  # 一定要排序，记得要排序
    par_cnt = par_cnt if par_cnt < len(all_par) else len(all_par)
    gap = round(len(all_par) / par_cnt)
    all_par = [all_par[i: i + gap] for i in range(0, len(all_par), gap)]
    print('一共有%d个分区' % len(all_par))
    # 每次取cnt个分区出来
    all_files = []
    file_id = 1
    for sub_par in all_par:
        # 取出子集
        min_, max_ = min(sub_par), max(sub_par)
        if len(sub_par) == 1:
            sub_df = df.loc[df['hash_col'] == min_]
        else:
            sub_df = df.loc[(df['hash_col'] >= min_) & (df['hash_col'] <= max_)]
        del sub_df['hash_col']
        # 保存
        if to_pickle:
            subfile_name = os.path.join(path, prefix + '_%03d.pickle' % file_id)
        else:
            subfile_name = os.path.join(path, prefix + '_%03d.csv' % file_id)
        if os.path.exists(subfile_name):
            os.remove(subfile_name)
        # 是否保存为pickle
        if to_pickle:
            with open(subfile_name, 'wb') as f:
                pickle.dump(sub_df, f)
        # 保存到csv
        else:
            sub_df.to_csv(subfile_name, index=False)
        all_files.append(subfile_name)
        print('分区数据 %s 保存到 %s，数据量：%d' % (str(sub_par), subfile_name, len(sub_df)))
        file_id += 1
    # 返回
    return all_files


def df_partition_not_to_file(df, id_col=None, par_cnt=10):
    """将dataframe分拆成多个子df，要求相同的id在同一个子df中"""
    # import numpy as np
    # import pandas as pd
    # from numpy import linspace
    # df = pd.DataFrame(np.random.random((20,5)), columns=['c%d'%i for i in range(5)])
    # id_col=['c0','c1']
    # par_cnt=3
    # 创建tmp列，用来计算hash分区id
    if isinstance(id_col, str):
        df['hash_col'] = df[id_col].apply(lambda x: int(str(hash(x))[-2:]))  # 转哈希后取后两位数字用于分区
    else:
        df['tmp_hash_col'] = ''
        for col in id_col:
            df['tmp_hash_col'] = df['tmp_hash_col'] + df[col].apply(str)
        df['hash_col'] = df['tmp_hash_col'].apply(lambda x: int(str(hash(x))[-2:]))  # 转哈希后取后两位数字用于分区
        del df['tmp_hash_col']
    # 计算分区数量
    all_par = df['hash_col'].unique()
    all_par.sort()  # 一定要排序，记得要排序
    par_cnt = par_cnt if par_cnt < len(all_par) else len(all_par)
    gap = round(len(all_par) / par_cnt)
    all_par = [all_par[i: i + gap] for i in range(0, len(all_par), gap)]
    print('一共有%d个分区' % len(all_par))
    # 每次取cnt个分区出来
    all_sub_data = []
    file_id = 1
    for sub_par in all_par:
        # 取出子集
        min_, max_ = min(sub_par), max(sub_par)
        if len(sub_par) == 1:
            sub_df = df.loc[df['hash_col'] == min_]
        else:
            sub_df = df.loc[(df['hash_col'] >= min_) & (df['hash_col'] <= max_)]
        del sub_df['hash_col']
        all_sub_data.append(sub_df)
        file_id += 1
    # 返回
    return all_sub_data


def _test_df_partition():
    import numpy as np
    df = pd.DataFrame(np.random.random(20, 5), columns=['a'])
    par_cnt = 3
    id_col = 'a'
    path = r'E:\data_tmp\data_path'
    prefix = 'test_'
    df_partition(df=df, id_col=id_col, path=path, prefix=prefix, par_cnt=par_cnt)
