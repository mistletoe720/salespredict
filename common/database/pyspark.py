# -*- coding: utf-8 -*-
"""
spark-python连接
"""
import re
import time
import datetime
import os
import threading
import subprocess
from .pyyarn import pyyarn as pyyarn_
from ...config import config

# spark集群总资源,内存3.12T
TOTAL_MEMORY = 3.12 * 1000
# CPU核心数
TOTAL_CORES = 512

# 多线程执行标志 -2没执行，-1正在执行，0失败，1成功
thread_sign = {}  # thread_sign.get(key,-2)
thread_lock = threading.Lock()  # 线程锁


def now_str():
    return '[%s]' % str(datetime.datetime.now())[:19]


class pyspark():
    """spark相关连接"""

    def __init__(self, queue='ai'):
        self.queue = queue

    def to_log(self, string, log=None):
        """打印情况"""
        log.info(string) if log else print(log)

    def thread_sign_change(self, name, param_value):
        """修改多线程通信参数"""
        global thread_sign, thread_lock
        thread_lock.acquire()
        thread_sign[name] = param_value
        thread_lock.release()
        value = {-2: '未执行', -1: '正在执行', 0: '失败', 1: '成功'}
        t = str(datetime.datetime.now())[:19]
        print('%s 将多线程通信状态改成：%s = %d(%s)' % (t, name, param_value, value[param_value]))

    def thread_sign_get(self, name):
        """获取多线程通信参数"""
        global thread_sign
        return thread_sign.get(name, -2)

    def now(self):
        return str(datetime.datetime.now())[:19]

    def keep_n_days_log(self, path, prefix, suffix='.log'):
        """保留n天的日志"""
        keep = 15  # 保留15天的日志
        files = [file for file in os.listdir(path) if file.startswith(prefix) and file.endswith(suffix)]
        last_n_day = str(datetime.datetime.today() - datetime.timedelta(keep))[:10]
        oldest_log = prefix + last_n_day
        for file in files:
            if file < oldest_log:
                os.remove(os.path.join(path, file))
        return

    def print_error_log(self, log_file):
        """获取错误日志的最后几行"""
        with open(log_file, 'r', encoding='utf-8') as f:
            logs = f.readlines()
        logs = logs[-500:]
        logs2 = ''
        ss = re.compile('\d{2,4}/\d\d/\d\d \d\d:\d\d:\d\d')
        for line in logs:
            if len(ss.findall(line)) == 0:
                # logs2 = logs2 + '\n' + line
                logs2 = logs2 + line
        print('=' * 200)
        print('spark执行失败，错误信息如下：')
        print(logs2)
        print('=' * 200)

    def spark_run_app(self, job_name, pyfile, params, need, endtime=None, timeout=24 * 3600, retry=3):
        """
        执行spark程序，以前已经有一个spark_run_app函数，故因此命名为spark_run_app2
        :param job_name: spark任务的名称
        :param pyfile: python文件
        :param params: spark的参数
        :param need: 需要的内存量，单位GB
        :param endtime: 允许最迟开始时间
        :param timeout: spark执行最长时间（秒）,超时则杀死重试
        :param retry: 失败重试次数
        """
        yarn = pyyarn_()
        # 判断日志目录是否存在
        if not os.path.exists(config.log_path):
            t = str(datetime.datetime.now())[:19]
            print('%s 没有那个目录，将创建：%s' % (t, config.log_path))
            os.makedirs(config.log_path)
        #
        # 删除旧的日志，保留n天
        prefix = os.path.split(pyfile)[-1].split('.')[0] + '--spark-log--'  # 日志名格式：文件名-log-2018-07-05-12:12:12.log
        self.keep_n_days_log(path=config.log_path, prefix=prefix, suffix='.log')
        # 创建本次日志文件名
        now = datetime.datetime.now().strftime('%Y-%m-%d-%H:%M:%S')
        log_file = os.path.join(config.log_path, '%s%s.log' % (prefix, now))
        #
        # 组装spark提交语句
        cmd = config.spark_submit_py3
        cmd = cmd.replace('{job_name}', job_name). \
            replace('{other_params}', params). \
            replace('{python_file}', pyfile). \
            replace('{log_file}', log_file)
        # 先判断yarn资源情况，如果够用，则启动
        done, code = yarn.wait_for_yarn_memory(need=need, endtime=endtime, queue='ai', waite=20)
        if not done:
            raise Exception(code)
        # 开始
        try_cnt = 1
        while try_cnt <= retry:
            use_time = 0
            print('\n%s\n第 %d 次执行spark任务：\n%s \n%s\n' % ('*' * 150, try_cnt, cmd, '*' * 150))
            ps = subprocess.Popen([cmd], shell=True)
            while use_time <= timeout:
                # 任务完成
                if ps.poll() == 0:
                    print(now_str(), 'spark任务：%s 完成，耗时：%d 秒' % (job_name, use_time))
                    return 1
                if ps.poll() and ps.poll()>0:
                    print(now_str(), 'spark任务错误，错误提示如下：')
                    self.print_error_log(log_file)
                # 任务未完成也没有超时
                if yarn.application_exists(self.queue, job_name):
                    print(now_str(), '在队列 %s 中找到任务：%s，但没有超时，等待30秒后再次查询' % (self.queue, job_name))
                # 任务没启动
                if not yarn.application_exists(self.queue, job_name):
                    print(now_str(), '在队列 %s 中没有找到任务：%s，请检查任务是否已经启动，等待30秒后再次查询' % (self.queue, job_name))
                # 等待下次
                time.sleep(30)
                use_time += 30
            # 任务超时，杀死任务
            yarn.kill_application(queue=self.queue, job_name=job_name)
            time.sleep(5)
            print(now_str(), '第 %d 次尝试任务超时，任务被杀死' % try_cnt)
            self.print_error_log(log_file)
            try_cnt += 1
        # 重试三次都失败
        print(now_str(), '重试 %d 次后spark任务仍然失败，不再重试' % retry)
        return 0

    def spark_run(self, pyfile, params, need, endtime=None, job_name=None, timeout=24 * 3600, retry=3):
        """提交spark任务，使用多线程，一个线程执行spark，另一个线程检测spark的执行状态，如果运行太久，
        则杀死，然后主程序重试。
        :param pyfile: 本地Python全文件路径
        :param params: spark submit 提交参数
        :param need: 执行spark申请的内存GB
        :param endtime: 资源判断等待时间，超过某个时间点未获取够资源则报错，'12:59:59;
        :param job_name: spark任务执行名称
        :param timeout: spark执行最长时间（秒）,超时则杀死重试
        :param retry: 失败重试次数
        :return: 成功：1 ； 失败：0
        """
        # jobs = []
        # # 启动spark提交任务
        # t1 = threading.Thread(target=self.spark_run_app, args=(pyfile, params, need, endtime))
        # jobs.append(t1)
        # # 判断spark任务超时，则kill掉
        # t2 = threading.Thread(target=self.spark_kill_app, args=(job_name,timeout))  # 允许最长执行30分钟，超时杀死
        # jobs.append(t2)
        if not job_name:
            job_name = now_str()
        return self.spark_run_app(job_name=job_name, pyfile=pyfile, params=params, need=need,
                                  endtime=endtime, timeout=timeout, retry=retry)


def _test():
    spark = pyspark()
    pyfile = 'pyfile.py'
    params = 'params'
    need = 10
    endtime = None
    job_name = 'job_name'
    timeout = 30

    spark.spark_run(pyfile, params, need, endtime, job_name, timeout, retry=3)
