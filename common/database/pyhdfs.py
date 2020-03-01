# -*- coding: utf-8 -*-
"""
使用python操作hdfs的文件,主要是上传和下载文件，这样比从hive读取会快很多.
如果是本地windows电脑拉取，则还需要配置：C:\Windows\System32\drivers\etc\hosts


pip install hdfs

host='172.31.100.170'
port='50070'
from hdfs import Client
conn = Client("http://172.31.100.169:50070",root="/",timeout=100,session=False)


方法2：
第三方模块使用pyhdfs实现，而不是hdfs，好处是，一次传入所有ip，而不是只能传入正在工作的节点ip
import pyhdfs
hosts = ','.join(['%s:%s' % (v, port) for k, v in cluster_ip.items()])
# 172.31.100.169:50070,172.31.100.170:50070,172.31.100.168:50070
fs = pyhdfs.HdfsClient(hosts=hosts)  # _pyhdfs 即 pyhdfs，需要重命名，否则和类名相同
fs.listdir('/')


# 需要注意的是：

从hdfs读取txt,csvd等结构化数据到dataframe，注意，不能是parquet等格式数据.
注意，该函数只读取csv, txt类型的hdfs数据，其他不管，csv或txt应该以逗号作为分隔符， 而且hdfs文件应该是包含表头的（hive表除外）.

该csv应该是中等大小数据集，能够在python内存进行csv的各种操作.

你应该知道的是，如果直接返回dataframe，所有的数据类型都是string.
因此要求，hdfs文件的数据，是非常标准，规范的数据，最好是全部的字段都是数值型，如果有字符串字段，要求保证没有逗号存在该字段中，切记切记切记。

此外，你需要注意的是，一般hdfs_path路径看起来是一个文件名路径，其实是一个hdfs文件夹，里面有很多小文众多小文件最终组成一个我们看到的简单的csv文件。

如果用spark保存到hdfs时应该指定字段：
    dataframe.write.mode("overwrite").options(header="true").csv("/home/xx.csv")
使用hive指定分隔符，而且保存格式为text file：
    create table tb_name row format delimited fields terminated by ',' stored as TEXTFILE  as select * from xxx

"""
import datetime
import time
import pandas as pd
import os
import shutil
import hdfs
from subprocess import check_output
from ...config.config import cluster_ip
from ...config.config import ex_data

class pyhdfs():
    def __init__(self, host='10.1.101.2', port='50070', encrypt=False):
        self.host = host
        self.port = port
        self.encrypt = encrypt
        self.conn = self._init_connect()

    def to_log(self, string, log=None):
        """打印"""
        log.info(string) if log else print(string)

    def _init_connect(self):
        """连接hdfs服务器"""
        # 先获取hdfs ip
        ips = sorted([ip for hostname, ip in cluster_ip.items()])
        port = self.port  # port='50070'
        conn = None
        for host in ips:
            # 连接字符串
            url = "http://{host}:{port}".format(host=host, port=port)
            try:
                conn = hdfs.Client(url, root='/', timeout=100, session=False)
                conn.list('/')
                break
            except:
                pass
        # 如果执行到这一步，说明前面的都有问题
        if conn.list('/'):
            return conn
        else:
            raise Exception('没有找到hdfs的地址，请检查')

    def ping(self):
        """检查连接，如果连接断开，则重新连接"""
        try:
            self.conn.list('/')
            return  # 此处返回
        except:
            pass
        # 如果连接断开，则重新连接
        cnt = 1
        while cnt < 10:
            try:
                self.conn = self._init_connect()
                print('检测到hdfs连接断开，重新连接%d次后成功' % cnt)
                return  # 此处返回
            except:
                print('检测到hdfs连接断开，重新连接第%d次失败，等待10秒后重试' % cnt)
                time.sleep(10)
        # 10次后失败，报错
        raise Exception('检测到hdfs连接断开，重新连接第%d次仍失败，请检查' % cnt)

    def list(self, hdfs_path):
        """查看当前目录下面有哪些文件"""
        return self.conn.list(hdfs_path)

    def upload(self, *args, **kwargs):
        """上传数据"""
        return self.conn.upload(*args, **kwargs)

    def makedirs(self, *args, **kwargs):
        """创建目录"""
        return self.conn.makedirs(*args, **kwargs)

    def exists(self, hdfs_path):
        """判断hdfs的目录或者文件是否存在"""
        try:
            self.conn.status(hdfs_path)
            return 1
        except:
            return 0

    def isdir(self, hdfs_path):
        """判断是否目录"""
        try:
            self.conn.list(hdfs_path)  # 是目录
            return 1
        except:
            return 0  # 不是目录

    def get_hive_tb_columns(self, tb_name):
        """获取hive表的字段名，在读取hive表的时候可能有用"""
        cmd = "hive -e 'SHOW COLUMNS FROM  {tb}' ".format(tb=tb_name)
        result = check_output([cmd], shell=True)
        columns = [col for col in result.decode().lower().replace(' ', '').split('\n') if col]
        print('获取HIVE表 %s 字段名：%s' % (tb_name, str(columns)))
        return columns

    def download(self, hdfs_path, local_path, overwrite=True, n_threads=1):
        """
        从hdfs下载数据,如果本地已存在该文件夹，则删除改文件夹
        :param hdfs_path: hdfs的文件路径
        :param local_path: 存放的本地路径，是一个文件夹
        :param overwrite: Overwrite any existing file or directory.
        :param n_threads: 开启多线程下载，当数据很多的时候可以使用
        :return:
        """
        # 查看远程路径是否存在
        try:
            self.conn.status(hdfs_path)
        except Exception as e:
            raise Exception('hdfs上没有这个东西：%s，请检查' % hdfs_path)
        # 查看本地路径是否存在，如果存在则删除
        if os.path.exists(local_path) and os.path.isdir(local_path):
            self.to_log('本地目录已经存在，将删除：' + local_path)
            shutil.rmtree(local_path)
        # 开始下载
        self.conn.download(hdfs_path, local_path, overwrite=overwrite, n_threads=n_threads)
        self.to_log('文件已经下载到：' + local_path)

    def load_csv_from_hdfs(self, hdfs_path, local_path, file_name, log=None):
        """
        从hdfs下载数据, 需要注意的是，一般我们用到这个函数的时候，是下载csv类型的文件。
        下载下来的是一堆小文件，需要合并成一个大文件，因此需要指定分隔符。
        :param hdfs_path:
        :param local_path:
        :return:
        步骤：
        1.下载数据到本地临时文件夹
        2.合并数据文件成一个大的文件
        3.删除本地临时文件夹
        """
        info = """ 
        ----------------------------------------------------------------------------------------------
        注意，该函数只读取csv类型的hdfs数据，其他不管，csv应该以逗号作为分隔符。
        该csv应该是中等大小数据集，能够在python内存进行csv的各种操作.
        如果用spark保存到hdfs时应该指定字段：
            dataframe.write.mode("overwrite").options(header="true").csv("/home/xx.csv")
        从hdfs拉取下来的文件，应该是.csv结尾，否则不能识别
        ----------------------------------------------------------------------------------------------
        """
        t1 = time.clock()
        self.to_log(info, log)
        # 下载数据
        files_dir_path = os.path.join(local_path, file_name) + '_tmp_dir'
        self.download(hdfs_path, files_dir_path)
        # 合并数据
        files = [file for file in os.listdir(files_dir_path) if file.endswith('.csv')]
        files = [os.path.join(files_dir_path, file) for file in files]

        data = pd.DataFrame()
        for file in files:
            subdata = pd.read_csv(file)
            if len(subdata)==0:
                self.to_log('hdfs分区文件(%s)没有数据，继续读取下一个'%file)
                continue
            data = pd.concat([data, subdata])
        t2 = time.clock()
        self.to_log('读取 %s，数据量：%d，耗时：%.2f 秒' % (files_dir_path, len(data), (t2 - t1)), log)
        # 写到本地
        file_path2 = os.path.join(local_path, file_name)
        data.to_csv(file_path2, index=False)
        self.to_log('数据保存到：' + file_path2)
        # 删除临时文件夹
        shutil.rmtree(files_dir_path)
        self.to_log('删除临时文件夹：' + files_dir_path, log)
        # 返回下载后的文件名
        return file_path2

    def read_csv_from_hdfs(self, hdfs_path, local_path=None, header=None, log=None, text_end=False):
        """
        从hdfs上面直接下载数据，一般是下载一个文件夹，然后将文件夹里面的数据文件整合成一个大的dataframe。
        和 load_csv_from_hdfs 函数不同的是，read是直接读取hdfs文件数据，然后在内存中转成dataframe
        load是现将数据文件保存到本地，然后再读取到python，整合成一个大的dataframe，再写到文件，来回倒腾。
        :param hdfs_path: hdfs路径
        :param local_path: 如果指定，则保存到本地，Nne则返回dataframe给其他模块
        :param header: 如果csv是没有表头的，则需要传入表头,list类型，header=['a','b','c',...]
        :param log:
        :param text_end: 仅能识别txt或csv结尾的数据文件
        :return:
        dataframe.repartition(1).write.mode("overwrite").options(header="true").csv("/home/xx.csv")
        """
        self.ping()
        print('-' * 100)
        t1 = time.clock()
        conn = self.conn
        # 检查是否存在
        if not conn.status(hdfs_path):
            raise Exception('hdfs上没有这个目录，请检查：' + hdfs_path)
        # 检查传进来的是文件名还是路径名
        status = conn.status(hdfs_path)
        if status['type'] == 'FILE':
            raise Exception('传进来的是文件名称，不是hdfs路径，请检查：' + hdfs_path)
        # 循环读取数据,之所以用循环，特别是hive表可能是分区表的时候很麻烦
        # 这里千万注意，一定要是linux的目录分隔符格式，即/，而不是\\
        all_files = list(conn.walk(hdfs_path))
        all_files2 = []
        for path, partition, files in all_files:
            files2 = ['%s/%s'%(path, file) for file in files if '_SUCCESS' not in file]
            all_files2.extend(files2)
        # 筛选要求csv或txt结尾
        if text_end==True:
            all_files2 = [file for file in all_files2
                        if file.lower().endswith('txt') or file.lower().endswith('csv')]
        print('hdfs：%s 下一共有%d个文件是txt或者csv' % (hdfs_path, len(all_files2)))
        # 读取数据
        if not header:
            print('没有传入字段名，将使用第一行作为dataframe的字段名')
        all_data = []
        for file in all_files2:
            # 直接读取数据，是byte类型
            with conn.read(file) as reader:
                data = [row.split(',') for row in reader.read().decode().split('\n') if row]  # 排除空行
            if len(data)==0:
                print('hdfs分区文件(%s)没有数据，继续读取下一个'%file)
                continue
            # 转成dataframe
            if header:
                data2 = pd.DataFrame(data, columns=header)
            else:
                data2 = pd.DataFrame(data[1:], columns=data[0])  # 默认第一行的表头，2-n行是数据
            all_data.append(data2)
        # 将所有数据子集合并成一个大的dataframe
        all_data2 = pd.concat(all_data)
        t2 = time.clock()
        print('下载并将所有子文件合并成一个大的dataframe，数据量：%d，耗时：%d 秒' % (len(all_data2), t2 - t1))
        # 是否返回，如果传入保存路径则保存到数据文件，否则就直接返回dataframe
        if not local_path:
            tmp_file_path = os.path.join(ex_data,'_read_csv_from_hdfs_%s.csv'%datetime.datetime.now().strftime('%Y%m%d_%H%M%S'))
        else:
            tmp_file_path = local_path
        p_path = os.path.dirname(tmp_file_path)
        if not os.path.exists(p_path):
            print('目录：%s 不存在，将创建' % p_path)
            os.makedirs(p_path)
        all_data2.to_csv(tmp_file_path, index=False)
        print('数据保存到本地文件：' + tmp_file_path)
        print('-' * 100)
        if local_path:
            return local_path
        else:
            # 如果直接返回dataframe，数据格式都是字符串，要保存到csv再读取才是正确的格式
            # 从csv再次读取
            all_data2 = pd.read_csv(tmp_file_path)
            # 删除文件
            os.remove(tmp_file_path)
            print('删除本地文件：' + tmp_file_path)
            print('-' * 100)
            return all_data2

    def read_hive_data_from_hdfs(self, hive_table, header=None, local_path=None):
        """
        从hdfs读取hive数据，而不是通过hive session，注意，只能读取存储为text类型的hive表
        原理和read_csv_from_hdfs是一样的，只不过在外部修改hdfs路径为hive路径唯一.
        :param hdfs_path: hive表名
        :param header: 如果csv是没有表头的，则需要传入表头,list类型，header=['a','b','c',...]
                        一般来说，hive表的hdfsfile都是没有表头的.
        :param local_path: 如果指定，则保存到本地，Nne则返回给其他模块
        :param log:
        :return:
        """
        self.ping()
        print('-' * 100)
        if not header:
            raise Exception('你应该传入hive表头')
        hdfs_path = '/apps/hive/warehouse/{db_name}.db/{tb_name}'.format(
            db_name=hive_table.split('.')[0],tb_name=hive_table.split('.')[-1])
        all_data2 = self.read_csv_from_hdfs(self, hdfs_path=hdfs_path, header=header, text_end=False)
        # 是否返回
        if local_path:
            all_data2.to_csv(local_path, index=False)
            print('数据保存到本地文件：' + local_path)
            print('-' * 100)
            return local_path
        else:
            print('-' * 100)
            return all_data2

def test():
    # 连接
    from .common.database.pyhdfs import pyhdfs
    conn = pyhdfs()
    # 查看文件
    conn.list('/usr/local/workspace')
    # 直接从hdfs下载文件到本地目录
    conn.load_csv_from_hdfs(hdfs_path='/usr/local/workspace/.csv',
                            local_path=r'/home/',
                            file_name='s.csv')
    # 从hdfs读取文件到本地，而不是下载到本地后再读取整合
    data = conn.read_csv_from_hdfs(hdfs_path='/apps/hive/warehouse/g')
    # 上传文件
    conn.list('/apps/hive/warehouse/tmp.db/tmp')
    conn.conn.delete('/apps/hive/warehouse/tmp.db/tmp.csv')
    conn.conn.upload(hdfs_path='/apps/hive/warehouse/tmp.csfs', local_path=r'/home/tb_name.csv')
    conn.conn.status('/apps/hive/warehouse/tmp.test')
