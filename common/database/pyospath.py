"""
与目录和文件操作相关的快捷函数
"""
import os
import shutil
import platform
import pickle
import json



def system_type():
    """
    判断操作系统类型。
    只有两种类型：windows 和 linux
    :return:
    """
    return platform.system().lower().strip()


def get_name(path, types='file'):
    """从文件路径解析文件名或者文件夹名"""
    # path=r'E:\banggood_python\aipurchase\business\predict\predict.py'
    if types=='file':
        return os.path.basename(path)
    if types=='dir':
        return os.path.dirname(path)


def mkdir_if_not_exists(path, overwrite=False):
    """如果目录不存在，将村建"""
    # 如果不存在，则创建
    if not os.path.exists(path):
        os.makedirs(path)
        print('目录不存在，将创建：' + path)
        return
    # 如果存在同名的文件，判断是否需要强制删除文件后创建文件夹
    if os.path.isfile(path) and overwrite:
        os.remove(path)  # 先删除文件
        os.mkdir(path)  # 再创建文件夹
    # 如果存在同名文件，但是不允许覆盖重写，就报错
    elif os.path.isfile(path) and not overwrite:
        raise Exception('该目录下已经存在同名的文件，不可创建与文件同名的文件夹，'
                        'overwrite=True可以先删除该文件后创建：' + path)


def remove_files(path, file_prefix=None, file_suffix=None):
    """删除指定前缀或者后缀的文件，如果不指定，就删除全部"""
    # file_prefix='__init'
    # file_suffix='.py'
    # 判断是否存在
    if not os.path.exists(path):
        print('指定的目录不存在，不需要执行删除操作：' + path)
    # 获取全部文件
    all_files = os.listdir(path)
    files2 = []
    # 删除前缀文件
    if file_prefix:
        files = [file for file in all_files if file.startswith(file_prefix)]
        files2.extend(files)
    # 删除后缀文件
    if file_suffix:
        files = [file for file in all_files if file.endswith(file_suffix)]
        files2.extend(files)
    # 删除全部文件
    if not file_prefix and not file_suffix:
        files2 = all_files
    # 开始删除文件
    for file in set(files2):
        file = os.path.join(path, file)
        os.remove(file)


def remove_path_and_files(path):
    """删除目录以及子目录子文件等"""
    # path=r'D:\a'
    if not os.path.exists(path):
        print('指定的目录不存在，不需要执行删除操作：' + path)
    # 删除
    shutil.rmtree(path)



def pickle_file(path, io='read', data=None, overwrite=False):
    """读写pickle文件"""
    # data = {'a':123,'b':'a'}
    # path=r'D:\a\aa\a123'
    # force_write=True
    # 读pickle文件
    if io=='read':
        if not os.path.exists(path):
            raise Exception('找不到文件：'+path)
        with open(path,'rb') as f:
            data = pickle.load(f)
            return data
    # 写pickle文件 io='write'
    if io=='write':
        # 存在，但是不能强制覆盖
        if os.path.exists(path) and not overwrite:
            raise Exception('已经存在该文件，请确认是否覆盖写入')
        # 存在，但是允许覆盖，则先删除该文件或者文件夹
        if os.path.exists(path) and overwrite:
            if os.path.isdir(path):
                shutil.rmtree(path)
            if os.path.isfile(path):
                os.remove(path)
        # 如果不存在，则判断是否需要建立文件夹
        p_path = os.path.dirname(path)
        if not os.path.exists(p_path):
            os.makedirs(p_path)
            print('目录不存在，已经创建：'+p_path)
        # 最后才是写
        with open(path, 'wb') as f:
            pickle.dump(data, f, protocol=4)

def json_file(path, io='read', data=None, overwrite=False):
    """读写pickle文件"""
    # import numpy as np
    # import pandas as pd
    # data = pd.DataFrame(np.random.rand(5, 5), columns=['a', 'b', 'c', 'd', 'e'])
    # path=r'D:\a\aa\a123'
    # force_write=True
    # 读pickle文件
    if io=='read':
        if not os.path.exists(path):
            raise Exception('找不到文件：'+path)
        with open(path,'r') as f:
            data = json.load(f)
            return data
    # 写pickle文件 io='write'
    if io=='write':
        # 存在，但是不能强制覆盖
        if os.path.exists(path) and not overwrite:
            raise Exception('已经存在该文件，请确认是否覆盖写入')
        # 存在，但是允许覆盖，则先删除该文件或者文件夹
        if os.path.exists(path) and overwrite:
            if os.path.isdir(path):
                shutil.rmtree(path)
            if os.path.isfile(path):
                os.remove(path)
        # 如果不存在，则判断是否需要建立文件夹
        p_path = os.path.dirname(path)
        if not os.path.exists(p_path):
            os.makedirs(p_path)
            print('目录不存在，已经创建：'+p_path)
        # 最后才是写
        with open(path, 'w') as f:
            json.dump(data, f)





def test():
    pass