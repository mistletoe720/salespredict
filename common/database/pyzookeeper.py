# -*- coding: utf-8 -*-
"""
python连接zookeeper集群
"""
from kazoo.client import KazooClient
from ...config.config import cluster_ip

class pyzookeeper():
    """python连接zookeeper集群"""
    def __init__(self, host=None, port='2181', read_only=True):
        self.host=host
        self.port=port
        self.read_only=read_only
        self.conn = self._init_connect()

    def _init_connect(self):
        """连接集群"""
        # ip转成list格式 [127.0.0.1]
        if self.host and isinstance(self.host, list):
            ips=self.host
        if self.host and isinstance(self.host, str):
            ips=[self.host]
        if not self.host:
            ips=[ip for hostname,ip in cluster_ip.items()]
        # 转成ip字符串
        # hosts = ''
        ips = ['%s:%s' % (ip, self.port) for ip in ips]
        ips = ','.join(ips)
        print(ips)
        # 连接
        conn = KazooClient(hosts=ips, read_only=self.read_only)
        conn.start()
        return conn

    def get_root(self):
        """获取根目录的所有节点,方便开始工作"""
        try:
            return 1, self.conn.get('/')
        except:
            return 0, ''

    def get_child(self, path):
        """获取当前节点的子节点，返回list"""
        try:
            return 1, self.conn.get_children(path)
        except:
            return 0, []

    def close(self):
        """关闭"""
        self.conn.stop()
        self.conn.close()


def test():
    zk=pyzookeeper()
    zk.get_root()
    zk.get_child('/hadoop-ha/ActiveBreadCrumb')