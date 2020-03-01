# -*- coding: utf-8 -*-
"""
使用redis数据库作为消息队列存储.
redis可以创建多个库，通过db=int指定库序号

由于将python的字典等数据存入redis后再取出来就是bytes，所以需要decode才能转回string，在dict转回字典

redis中列表的操作，这里约定，从左边插入，从右边取出,因此
index=0取出最后插入的元素（左边的元素）
index=-1取出最早插入的元素（右边的元素）


r = redis.Redis(host='192.168.8.143', port=6379)

"""
import redis

class RedisPool:
    """定义数据库连接池"""
    pool = None


class Redis(object):
    def __init__(self, host='127.0.0.1', port=6379, db=0):
        self.host = host
        self.port = port
        self.db = db
        self.conn = Redis.connect(self)

    def connect(self):
        """从连接池获取连接"""
        if RedisPool.pool == None:
            RedisPool.pool = redis.ConnectionPool(host=self.host, port=self.port, db=self.db)
        return redis.Redis(connection_pool=RedisPool.pool)


class RedisList(Redis):
    """针对列表操作的特殊类"""

    def __init__(self, host='127.0.0.1', port=6379, db=0):
        self.host = host
        self.port = port
        self.db = db
        self.conn = Redis.connect(self)

    def connect(self):
        """从连接池获取连接"""
        if RedisPool.pool == None:
            RedisPool.pool = redis.ConnectionPool(host=self.host, port=self.port, db=self.db)
        return redis.Redis(connection_pool=RedisPool.pool)

    def insert(self, list_name, value):
        """这里定义插入reids.list的函数，作为消息队列的容器，这里约定在左边插入"""
        self.conn.lpush(list_name, value)

    def get(self, list_name, index=-1, func=''):
        """定义从redis.list取出元素，这里默认取出最早插入的一个元素(右边的元素)"""
        value = self.conn.lindex(list_name, index)
        # 由于取出来的是bytes类型，需要转成字符型，然后转成dict，list等类型
        value = value.decode()
        if func:
            string = 'function(value)'
            string = string.replace('function', func).replace('value', value)
            return eval(string)
        return value

    def pop(self, list_name, index=-1):
        """get函数是取出list的值，而pop是删除list的值"""
        # 注意是删除左边还是右边的值
        if index == -1:
            self.conn.rpop(list_name)
        if index == 0:
            self.conn.lpop(list_name)


def main():
    pass


if __name__ == '__main__':
    main()
