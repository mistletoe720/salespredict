# -*- coding: utf-8 -*-
"""
与目录和文件操作相关的快捷函数
"""
import pickle
import json
import os
import platform
import shutil
import datetime
import sys as sys
from pandas import concat, DataFrame


####################################################################################################
class pyos():
    """与系统有关的一些命令封装"""

    def __init__(self):
        pass

    def system_type(self):
        """判断操作系统类型。只有两种类型：windows 和 linux"""
        return platform.system().lower().strip()

    def join_file_name(self, *args):
        """组装文件名称，相当于 os.path.join """
        return os.path.join(*args)

    def file_exists(self, path):
        """判断文件是否存在"""
        return os.path.exists(path)

    def get_file_name(self, path):
        """解析文件名"""
        return os.path.basename(path)

    def get_path_name(self, path):
        """解析文件的路径"""
        return os.path.dirname(path)

    def mkdir_if_not_exists(self, path, overwrite=True):
        """如果目录不存在，将创建，如果存在同名的文件，则删除该文件"""
        # 如果不存在，则创建
        if not os.path.exists(path):
            os.makedirs(path)
            print('目录不存在，将创建：' + path)
            return
        # 如果存在同名的文件，判断是否需要强制删除文件后创建文件夹
        if os.path.isfile(path) and overwrite:
            os.remove(path)  # 先删除文件
            os.mkdir(path)  # 再创建文件夹
            print('已删除同名文件，然后创建文件夹：%s' % path)
        # 如果存在同名文件，但是不允许覆盖重写，就报错
        elif os.path.isfile(path) and not overwrite:
            raise Exception('该目录下已经存在同名的文件，不可创建与文件同名的文件夹，'
                            'overwrite=True可以先删除该文件后创建：' + path)

    def remove_files_if_exists(self, path, file_prefix=None, file_suffix=None):
        """
        删除path下的指定格式文件。
        :param path: 目录或者文件名
        :param file_prefix: 如果文件名满足这个前缀，则删除
        :param file_suffix: 如果文件名满足这个后缀，则删除
        如果没有指定前缀和后缀，说明path的文件名，只需要删除这个文件
        """
        # file_prefix='__init'
        # file_suffix='.py'
        # 判断是否存在
        if not os.path.exists(path):
            print('指定的目录或者文件不存在，不需要执行删除操作：' + path)
            return
        # 如果是文件，则删除
        if os.path.isfile(path):
            os.remove(path)
            return
        # 否则就是删除指定前缀或者后缀的文件
        # 先获取全部文件
        all_files = os.listdir(path)
        files2 = []
        # 找到前缀格式文件
        if file_prefix:
            files = [file for file in all_files if file.startswith(file_prefix)]
            files2.extend(files)
        # 找到后缀格式文件
        if file_suffix:
            files = [file for file in all_files if file.endswith(file_suffix)]
            files2.extend(files)
        # 如果没有指定前缀或者后缀，就是删除全部文件
        if not file_prefix and not file_suffix:
            files2 = all_files
        # 开始删除文件
        for file in set(files2):
            file = os.path.join(path, file)
            os.remove(file)

    def remove_dir_and_files(self, path):
        """删除目录以及子目录子文件等"""
        # path=r'D:\a'
        if not os.path.exists(path):
            print('指定的目录不存在，不需要执行删除操作：' + path)
        # 删除
        shutil.rmtree(path)

    def remove_old_files(self, path, file_prefix, keep_days=5, datefmt='%Y-%m-%d.%H'):
        """删除旧的数据备份文件，要个要求文件命名格式： file_prefix_20150101 """
        # file_prefix = 'file_prefix_'
        # 判断路径是否存在
        if not os.path.exists(path):
            raise Exception('指定的路径不存在，请检查：%s' % path)
        # 找出指定前缀的文件
        files = [file for file in os.listdir(path) if file.startswith(file_prefix)]
        # 找到n天前的文件
        latest_day = (datetime.datetime.today() - datetime.timedelta(keep_days)).strftime(datefmt)
        old_file = file_prefix + latest_day
        remove_files = [file for file in files if file < old_file]
        # 开始删文件
        for file in remove_files:
            file = os.path.join(path, file)
            os.remove(file)
            print('删除旧的文件：%s' % file)
        return

    def back_file(self, file, suffix='_bak'):
        """备份文件"""
        file_bak = '%s%s' % (file, suffix)
        self.remove_files_if_exists(file_bak)
        shutil.copy(file, file_bak)

    def copy(self, source, target):
        """复制文件或文件夹到另外的地方"""
        if not os.path.exists(self.get_path_name(target)):
            os.makedirs(self.get_path_name(target))
            print('目标目录不存在，将创建：%s' % self.get_path_name(target))
        # 复制
        shutil.copyfile(source, target)

    def copy_dir(self, source, target):
        """复制文件夹"""
        if not os.path.exists(self.get_path_name(target)):
            os.makedirs(self.get_path_name(target))
            print('目标目录不存在，将创建：%s' % self.get_path_name(target))
        # 复制
        shutil.copytree(source, target)


####################################################################################################
class pypickle():
    """pickle工具封装"""

    def __init__(self):
        pass

    def mkdir_if_not_exists(self, path):
        """如果path的父目录不存在，就创建"""
        p_path = os.path.dirname(path)
        if not os.path.exists(p_path):
            os.makedirs(p_path)
            print('父目录不存在，将创建：%s' % p_path)

    def object_size(self, object):
        """对象的内存大小"""
        size = sys.getsizeof(object)
        b = 1
        kb = b * 2014
        mb = kb * 1024
        gb = mb * 1024
        if size > gb:
            return '%.2fGb' % (size / gb)
        if size > mb:
            return '%.2fMb' % (size / mb)
        if size > kb:
            return '%.2fKb' % (size / kb)
        else:
            return '%.2fByte' % (size / b)

    def read(self, path):
        """读取pickle数据"""
        if not os.path.exists(path):
            raise Exception('找不到文件：' + path)
        with open(path, 'rb') as f:
            data = pickle.load(f)
        return data

    def write(self, path, data):
        """将数据写到picker中"""
        # 存在，但是允许覆盖，则先删除该文件或者文件夹
        if os.path.exists(path) and os.path.isdir(path):
            raise Exception('指定的路径是已经存在的文件夹，贸然删除文件夹可能会导致问题，请手动删除或修改路径')
            # shutil.rmtree(path)
        if os.path.exists(path) and os.path.isfile(path):
            print('指定的文件已存在，将删除：' + path)
            os.remove(path)
        # 判断目录是否存在
        self.mkdir_if_not_exists(path)
        # 最后才是写
        with open(path, 'wb') as f:
            pickle.dump(data, f)

    def write_big_df(self, path, df, per_time=500000):
        """如果dataframe很大，比如300w*500的矩阵，占用10G内存，那将是很可怕的，所以需要另外处理，使用流式方法处理"""
        # 如果文件已存在，则删除
        if os.path.exists(path):
            os.remove(path)
            print('文件已存在，将删除：%s' % path)
        for i in range(0, len(df), per_time):
            with open(path, 'ab') as f:
                pickle.dump(df.iloc[i: i + per_time], f)
                print('将第 %d 个 %d 万行数据块序列化保存' % (i / per_time, per_time / 10000))
        return

    def read_big_df(self, path):
        """读取大dataframe的pickle文件，如果确实很大，内存很紧张，可能会出现内存错误问题，所以能不用就不用吧"""
        df = DataFrame()
        f = open(path, 'rb')
        try:
            i = 0
            while True:
                sub_df = pickle.load(f)
                df = concat([df, sub_df])
                cnt = len(sub_df)
                print('读取第 %.2f - %.2f 万行数据' % (i / 10000, (i + cnt) / 10000))
                i = i + cnt
        except:
            pass
        finally:
            f.close()
        return df


def _test_pickle():
    pypc = pypickle()
    # 测试写
    a = '这是测试数据'
    file = '/home/zhenyu/a.pickle'
    pypc.write(file, a)
    # 测试读
    b = pypc.read(file)
    print(b)


###############################################################################################
class pyjson():
    """json读写，主要针对dict数据"""

    def __init__(self):
        pass

    def read(self, path):
        """读"""
        if not os.path.exists(path):
            raise Exception('json文件不存在：%s' % path)
        if os.path.isdir(path):
            raise Exception('指定的path是一个目录，而不是json文件：%s' % path)
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
        return data

    def write(self, path, data):
        """写"""
        # 已经存在同名的文件夹，不能这样做
        if os.path.exists(path) and os.path.isdir(path):
            raise Exception('指定的路径是已经存在的文件夹，贸然删除文件夹可能会导致问题，请手动删除或修改路径')
        # if os.path.exists(path) and os.path.isfile(path):
        #     print('指定的文件已存在，将删除：' + path)
        #     os.remove(path)
        # 如果路径不存在，就尝试创建
        p_path = os.path.dirname(path)
        if not os.path.exists(p_path):
            os.makedirs(p_path)
        # 覆盖式写到文件
        with open(path, 'w', encoding='utf-8') as f:
            json.dump(data, f)
        return


def _test_json():
    pyjs = pyjson()
    # 测试写
    a = {'key': 'value'}
    file = '/home/zhenyu/a.json'
    pyjs.write(file, a)
    # 测试读
    b = pyjs.read(file)
    print(b)
###############################################################################################
