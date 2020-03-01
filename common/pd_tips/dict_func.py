# -*- coding: utf-8 -*-
"""
和字典有关的一些求和，差分，累积求和等简单数学操作
"""
from pandas import Series


def dict_float_round_2_string(_dict, n=3):
    """
    字典的值是小数，由于计算机存储精度问题，会导致float转string的时候带有很多0或者9，所以需要对转换后的数据处理。
    先对dict做roud四舍五入，然后再整体转成string。
    #
    _dict = {0: 0.0, 1: 3.5423999999999998, 2: 3.4751999999999996, 3: 3.4751999999999996, 4: 3.475200000000001}
    n=3
    """
    format_ = '.%df' % n
    return str(dict([[k, format(v, format_)] for k, v in _dict.items()])).replace("'", '')


def sum_dict(dicts):
    """
    对多个字典相同的键值求和
    :param dicts: list-dict
    :return:
    构造测试数据
    a={'a':1,'b':2}
    b={'b':3,'c':4}
    c={'e':5}
    dicts=[a,b,c]
    """
    sums = {}
    for d in dicts:
        for k, v in d.items():
            sums[k] = sums.get(k, 0) + d.get(k)
    return sums


def diff_dict(_dict):
    """
    对字典进行差分处理，和时间序列中差分一个意思.
    默认情况下，先从大到小排序，这样就和普通的时间序列一个理解方式了
    如果用pandas的diff是很方便，但是数据量很大的情况下就很慢了，要用迭代才行
    :param _dict: 字典类型
    :return:
    构造测试数据
    _dict={0:0.1, 1:1, 5:4, 2:3.3}
    """
    a = sorted(_dict.items(), key=lambda x: x[0])
    a2 = dict([[a[0][0], a[0][1]]] + [[v2[0], v2[1] - v1[1]] for v1, v2 in zip(a[:-1], a[1:])])
    return a2


def cumsum_dict(_dict):
    """
    对字典累积求和。
    :param _dict: 字典类型
    :return:
    构造测试数据
    _dict={0:0.1, 1:1, 2:3.3, 3:4 }
    """
    a = [[0, 0]]
    for k, v in sorted(_dict.items(), key=lambda x: x[0]):
        a.append([k, a[-1][-1] + v])
    a = dict(a[1:])
    return a


def scalar_mul(_dict, scalar):
    """
    一个常数乘以字典的每个值。
    :param _dict: 字典类型
    :param scalar: 常数
    :return:
    构造测试数据
    _dict={1:1, 3:4, 2:3}
    scalar=0.33333333
    """
    return dict([[k, v * scalar] for k, v in _dict.items()])


def vector_mul(dict1, dict2):
    """
    向量相乘，即对应的键值相乘
    :param dict1: 主字典1
    :param dict2: 辅字典2，即要求dict2的元素比dict1的元素多
    :return:
    dict1={1:2, 2:3, 3:4}
    dict2={1:1, 2:2, 3:3}
    """
    return (Series(dict1) * Series(dict2)).dropna().apply(lambda x: int(x * 1000) / 1000).to_dict()
    # new_dict = {}
    # for k, v in dict1.items():
    #     new_dict[k] = int(dict1[k] * dict2[k] *1000) / 1000
    # return new_dict


def dict_threshold(dict1, threshold, bigger=False):
    """
    dict1是一个销量预测累积值，现在要看在哪一天，其累计值刚好超过或小于预设的阈值。
    因此，要求键值对都是有序的。
    注意，dict1的键要求是int类型
    :param dict1: dict1={1:1.2, 2:8.1, 3:5.5, 4:6.7}
    :param threshold: threshold=3
    :param bigger: bigger=False, bigger=True 表示刚好<=阈值，True表示刚好>=阈值
    :return:
    dict_threshold(dict1, threshold, bigger=True)
    dict_threshold(dict1, threshold, bigger=False)
    """
    # 如果只有一个元素，好判断
    if len(dict1) == 0:
        return None, None
    if len(dict1) == 1:
        k, v = dict1.popitem()
        if bigger:  # 刚好大于阈值
            if threshold <= v:
                return k, v
            else:
                return None, None
        else:  # 刚好小于阈值
            if threshold >= v:
                return k, v
            else:
                return None, None
    # 刚好大于阈值
    if bigger:
        for k, v in sorted(dict1.items(), key=lambda item: item[0]):
            if v >= threshold:
                return k, v
        return None, None  # 如果到最后一个也没有大于阈值，那么就返回-1,-1
    # 刚好小于阈值
    if not bigger:
        for k, v in sorted(dict1.items(), key=lambda item: item[1], reverse=True):
            if v <= threshold:
                return k, v
        return None, None
