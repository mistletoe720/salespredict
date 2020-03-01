# -*- coding: utf-8 -*-
"""
python操作数据库PostgreSQL
"""

import base64
import psycopg2






def connect(host='',port='',user='', password='', database='',charset='',encrypt=True):
    '''连接mysql数据库'''
    # chartset='utf8',不是'utf-8'
    if encrypt:
        decode=lambda string: base64.b64decode(string).decode()
        host, port, user, passwd = decode(host), decode(port), decode(user), decode(password)
    if charset:
        conn = psycopg2.connect(host=host, port=port, user=user, password=password, database=database,charset=charset)
    else:
        conn = psycopg2.connect(host=host, port=port, user=user, password=password, database=database)
    return conn


