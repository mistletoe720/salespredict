# -*- coding: utf-8 -*-
"""
接收Email,发送Email的方法见另外一个py
"""
import imaplib
import base64
import email
from email.header import decode_header
from email.utils import parseaddr


# 定义邮件解析的函数

def guess_charset(msg):
    # 先从msg对象获取编码:
    charset = msg.get_charset()
    if charset is None:
        # 如果获取不到，再从Content-Type字段获取:
        content_type = msg.get('Content-Type', '').lower()
        pos = content_type.find('charset=')
        if pos >= 0:
            charset = content_type[pos + 8:].strip()
    return charset

def decode_str(s):
    value, charset = decode_header(s)[0]
    if charset:
        value = value.decode(charset)
    return value


def get_header(msg):
    """获取收发件人和标题"""
    title={}
    for header in ['From', 'To', 'Subject']:
        value = msg.get(header, '')
        if value:
            if header == 'Subject':
                # 需要解码Subject字符串:
                value = decode_str(value)
            else:
                # 需要解码Email地址:
                hdr, addr = parseaddr(value)
                name = decode_str(hdr)
                value = u'%s <%s>' % (name, addr)
        title[header.lower()]=value
    return title

def get_body(msg):
    """获取邮件正文,不包括附件"""
    all_content=[]
    for part in msg.walk():
        if not part.is_multipart():  # 如果ture的话内容是没用的
            content_type = part.get_content_type()
            if content_type == 'text/plain' or content_type == 'text/html':
                content = part.get_payload(decode=True).decode('utf-8')
                all_content.append(content)
    return '\n----------\n'.join(all_content)



class my_email():
    """收取EMAIL"""
    def __init__(self, user, passwd):
        self.user=user
        self.passwd=passwd
        self.conn=self._init_connect()

    def to_log(self, string, log=None):
        """打印"""
        log.info(string) if log else print(string)

    def _init_connect(self, log=None):
        """连接腾讯企业邮箱"""
        imap='imap.exmail.qq.com'
        conn=imaplib.IMAP4_SSL(imap)
        decode = lambda string: base64.b64decode(string).decode()
        result=conn.login(decode(self.user), decode(self.passwd))
        if result[0]=='OK':
            self.to_log('登录邮箱成功')
            return conn
        else:
            raise Exception('登录邮箱失败，请检查')

    def close(self):
        """关闭连接"""
        self.conn.close()
        self.conn.logout()

    def get_unseen_email(self, log=None):
        """获取未读邮件"""
        # self.conn.list()  # 查询所有维度邮件
        self.conn.select('inbox')  # 选择收件箱
        type, data = self.conn.search(None, 'UNSEEN')  # 选择所有未读
        self.to_log('当前有%d封未读邮件'%len(data),log)
        # 取出所有未读邮件
        all_email=[]
        for num in data[0].split():
            type, sub_data = self.conn.fetch(num, '(BODY.PEEK[])')  # 读取邮件
            msg = email.message_from_string(sub_data[0][1].decode('utf-8'))
            title=get_header(msg)  # 获取收发件人和标题
            text=get_body(msg)
            all_email.append([num, title, text])
            self.conn.store(num, '+FLAGS', '\Seen')  # 将收取后的邮件置为已读
        return all_email



def test():
    user=""
    passwd=""
    myemail=my_email(user,passwd)

    # 收取未读邮件
    all_email=myemail.get_unseen_email()
    print(all_email)
