# -*- coding: utf-8 -*-
"""
使用config作为消息传递的队列，即任务状态有变化，更新config.ini，否则不做修改

充当的角色相当于小型的redis消息队里，但是不能应对高并发写，会出现脏数据的.

既然是针对文件的读写，就需要有锁机制

"""
from configobj import ConfigObj
from filelock import Timeout, FileLock  # 文件锁
import os

class pyconfig():
    def __init__(self, config_file):
        self._config_file = config_file
        self._lock_file = None
        self._lock_timeout=3600    # 为了能让所有并发完成不出错，这里将超时时间设置大一些，一般情况下1秒就足够了
        self.create_lock_file()

    def create_lock_file(self):
        """创建锁的文件"""
        self._lock_file = self._config_file+'.lock'
        if not os.path.exists(self._lock_file):
            with open(self._lock_file, 'w', encoding='utf-8') as f:
                f.writelines('')
            print('文件锁不存在，将创建文件锁：%s'%self._lock_file)

    def get_param(self, pkey, defualt=''):
        """获取key的值"""
        lock = FileLock(self._lock_file)
        try:
            with lock.acquire(timeout=self._lock_timeout):
                pkey = str(pkey).lower().strip()
                config = ConfigObj(self._config_file, encoding='utf-8')
                pvalue = config.get(pkey, defualt)
                return pvalue
        except Timeout:
            raise Exception('文件锁超时')

    def update_param(self, pkey, pvalue):
        """更新key的值"""
        lock = FileLock(self._lock_file)
        try:
            with lock.acquire(timeout=self._lock_timeout):
                pkey = str(pkey).lower().strip()
                pvalue = str(pvalue).lower().strip()
                config = ConfigObj(self._config_file, encoding='utf-8')
                config[pkey] = pvalue
                config.write()
        except Timeout:
            raise Exception('文件锁超时')

    def get_param_with_status(self, pkey, status):
        """如果一个任务有多个状态，那么需要记录每个状态的时间"""
        pkey2 = '%s_____%s' % (str(pkey).lower().strip(), str(status))
        return self.get_param(pkey2)

    def update_param_with_status(self, pkey, status, pvalue):
        """更新某个键的某个状态的值"""
        pkey2 = '%s_____%s' % (str(pkey).lower().strip(), str(status))
        pvalue2 = str(pvalue).lower().strip()
        self.update_param(pkey2, pvalue2)

    def clear_param(self, pkey=None, all=False):
        """删除某个键"""
        lock = FileLock(self._lock_file)
        try:
            with lock.acquire(timeout=self._lock_timeout):
                pkey = str(pkey).lower().strip()
                config = ConfigObj(self._config_file, encoding='utf-8')
                for key, value in config.items():
                    if all:
                        config.pop(key)
                    elif key==pkey:
                        config.pop(key)
                config.write()
        except:
            raise Exception('文件锁超时')


def test_config():
    file = r''
    config = pyconfig(file)
    # 获取某个键的值
    config.get_param('abc')
    # 更新键的值
    config.update_param('abc', '123')

    # 更新任务完成时间
    config.update_param_with_status('abc', 's', 'today')
    # 获取任务完成时间
    config.get_param_with_status('abc', 's')

    # 删除某个键值
    config.clear_param('abc')
    # 删除全部键值
    config.clear_param(all=True)