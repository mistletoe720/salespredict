# -*- coding: utf-8 -*-
"""
格式化表格输出，更漂亮
"""


def calc_string_length(string):
    """计算字符串的长度，如果是汉子，为2字符长度"""
    cnt = 0
    for s in string:
        if '\u4e00' <= s <= '\u9fff':
            cnt += 2
        else:
            cnt += 1
    return cnt


def complete_str_length(string, length):
    """对string补齐到指定长度，填补为空字符串"""
    # 计算中文的个数
    cnt = 0
    for s in string:
        if '\u4e00' <= s <= '\u9fff':
            cnt += 1
    length = length - cnt
    return ('%%0%ds' % length) % string


def prettytable(df, max_value=60, each_row_cols=2):
    """格式化表格输出，当遇到中文时候就尴尬了，不一定能对齐"""
    # 判断标题和内容的长度那个长，将columns和values转成相同长度的字符串
    df = df.applymap(lambda x: ' ' + str(x)[: max_value] + ' ')
    df.columns = [' %s ' % c for c in df.columns]
    length1, length2, length = {}, {}, {}
    length1 = dict([[c, calc_string_length(c)] for c in df.columns])
    length2 = df.applymap(calc_string_length).max().to_dict()
    length = dict([[c, max(length1[c], length2[c])] for c in df.columns])
    # 补齐长度
    for c in df.columns:
        df[c] = df[c].apply(lambda x: complete_str_length(x, length[c]))
    df.columns = [complete_str_length(c, length[c]) for c in df.columns]
    # 再次取长度
    length = dict([[c, len(c)] for c in df.columns])
    # 每次取n列出来格式化
    all_string = ''
    for c_i in range(0, len(df.columns), each_row_cols):
        sub_df = df.iloc[:, c_i: c_i + each_row_cols]
        # 对表头处理
        col_str1 = '+'
        col_str2 = '|'
        for col in sub_df.columns:
            col_str1 += '-' * calc_string_length(col)
            col_str2 += col
            col_str1 += '+'
            col_str2 += '|'
        col_str = col_str1 + '\n' + col_str2 + '\n' + col_str1
        # 对表内容的处理
        data_str = ''
        for i in range(len(sub_df)):
            data_str += '|' + '|'.join(sub_df.iloc[i]) + '|\n'
        # 组装
        sub_all_string = col_str + '\n' + data_str + col_str1 + '\n'
        # print(sub_all_string)
        all_string += sub_all_string
    # print(all_string)
    return all_string


def _test_prettytable():
    import pandas as pd
    import numpy as np
    df = pd.DataFrame(np.random.rand(5, 6), columns=['aa', 'bb', 'cc', 'dd', 'ee', 'gg']).applymap(lambda x: int(x * 10))
    print(prettytable(df))
