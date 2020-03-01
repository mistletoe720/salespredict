# -*- coding: utf-8 -*-
"""
针对pandas的dataframe多列运算的语法糖
"""
import pandas as pd


def calc_dataframe_with_columns(func):
    """
    针对pandas的dataframe的多列运算的装饰器。
    是这样的一种情况，如果只是dataframe简单多列加减乘除，没问题，直接列运算就好了。
    但是如果涉及到判断等其他运算，就麻烦了。
    因此这里用装饰器，使用迭代表达式，对多列进行运算。
    最终返回一个dataframe，行数和索引和原来的dataframe一致。
    """

    def wrapper(*args, **kwargs):
        return pd.DataFrame([func(*x) for x in zip(*args)],
                            columns=['result'],
                            index=args[0].index)

    return wrapper


# ----------------------------------------------------

@calc_dataframe_with_columns
def myfunc(x, y, z):
    """定义一个简单函数，演示多列综合运算"""
    if x > 2:
        return y
    else:
        return z


def test():
    # 生成测试数据
    import pandas as pd
    import numpy as np
    df = pd.DataFrame(np.random.rand(3, 3) * 10,
                      columns=['a', 'b', 'c'],
                      index=['a', 'b', 'c']) \
        .applymap(int)

    myfunc(df['a'], df['b'], df['c'])
    myfunc(df['a'], df['c'], df['b'])
