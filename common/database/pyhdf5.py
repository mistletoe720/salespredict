"""
基于hdf5构建本地文件数据库，存储pandas dataframe。

使用场景是，有时候需要保存很多dataframe，而且是需要保存多天的dataframe，如果用csv或者pickle也行，就是不方便管理。

hdf5是一种高效压缩数据形式，可以有效存储numpy，且压缩效率杠杠滴。

hdf5的格式是：
 {  'key1': dataframe1,
    'key2': dataframe2,
    ....
}
需要注意是的，key的格式是 ^[a-zA-Z_][a-zA-Z0-9_]*$
即：大小写字母及下划线开头，然后是大小写字母和下划线。这种命名方式和python变量命名方式是一样的。

为了处理这种情况，这里偷偷处理，
1、所有的key都在前面加下划线，即 key-->_key

在这里的场景，一般情况下，key1是日期，也即是存储多天的日期数据。

约定，日期必须是 '20180101' 格式，不能是 ''的格式
"""
import pandas as pd
import os
from ...config.config import ex_data


class pyhdf5():
    def __init__(self, ex_data_path=None):
        self.ex_data_path = ex_data_path if ex_data_path else ex_data  # 数据存放目录

    def date_str(self, statedate, types='encode'):
        """将statedate的横线取消或者添加，因为pytable中的名称不能有横线"""
        if types == 'encode':
            return statedate.replace('-', '')  # 去掉横线，2018-10-10-->20181010
        if types == 'decode':
            return str(pd.to_datetime(statedate))[:10]  # 添加横线，20181010-->2018-10-10

    def get_file_path(self, path):
        """判断传进来的是文件名还是文件全路径，如果是文件名，就组装拼接成全路径"""
        # 传进来的是全路径文件
        if os.path.exists(path) and os.path.isfile(path):
            return path
        # 传进来的是文件名，需要加上路径
        if not os.path.exists(path):
            return os.path.join(self.ex_data_path, path)

    def new_key(self, key):
        """在key的前面添加下划线，key-->_key"""
        return '_' + key

    def get_keys(self, hdf5):
        """获取HDF5文件的所有keys"""
        hdf5 = self.get_file_path(hdf5)
        with pd.HDFStore(hdf5) as store:
            all_keys = [k[2:] for k in store.keys()]  # 每个key都删除前面的 '/_'
            return all_keys

    def read(self, hdf5, key='default'):
        """读取某个分区（key）的数据"""
        hdf5 = self.get_file_path(hdf5)
        key = self.new_key(key)
        with pd.HDFStore(hdf5) as store:
            return store[key]

    def write(self, hdf5, df, key='default'):
        """将dataframe写到某个分区下面"""
        hdf5 = self.get_file_path(hdf5)
        key = self.new_key(key)
        with pd.HDFStore(hdf5) as store:
            store[key] = df

    def delete(self, hdf5, key='default'):
        """删除某个分区下面的数据"""
        hdf5 = self.get_file_path(hdf5)
        key = self.new_key(key)
        with pd.HDFStore(hdf5) as store:
            del store[key]

    def delete_old_data(self, hdf5, statedate, keepdays=5):
        """对数据文件保留n天的历史数据"""
        # statedate='2018-10-15'
        # keepdays=5
        hdf5 = self.get_file_path(hdf5)
        keep_date = [str(day)[:10] for day in pd.date_range(end=statedate, periods=keepdays, freq='1D')]
        all_keys = self.get_keys(hdf5)
        delete_keys = [key for key in all_keys if key not in keep_date]
        with pd.HDFStore(hdf5) as store:
            for key in delete_keys:
                key = self.new_key(key)
                del store[key]


def _test():
    import pandas as pd
    import numpy as np
    #
    # 创建测试数据集
    df = pd.DataFrame(np.random.rand(5, 4), columns=['a', 'b', 'c', 'd']).applymap(lambda x: int(x * 10))
    df['a'] = df['a'].apply(str)
    df['statedate'] = '2018-10-10'

    # 实例化，执行数据存储的地方
    hdf5 = pyhdf5(ex_data)
    #
    # 将 2018-10-10 的数据写到h5中
    hdf5.write(hdf5='tb_name_h5.h5', df=df, key='2018-10-10')

    # 将 2018-10-11 的数据写到h5中
    df['statedate'] = '2018-10-11'
    hdf5.write(hdf5='tb_name_h5.h5', df=df, key='2018-10-11')

    # 读取某天的数据
    hdf5.read(hdf5='tb_name_h5.h5', key='2018-10-13')

    # 查看有多少天的数据
    hdf5.get_keys(hdf5='tb_name_h5.h5')

    # 保留1天的数据
    hdf5.delete_old_data(hdf5='tb_name_h5.h5', statedate='2018-10-11', keepdays=1)
