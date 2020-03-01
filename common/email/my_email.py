# # -*- coding: utf-8 -*-
# """
# 发送邮件的模块.
# 支持普通邮件和带附件的邮件，目前附件格式有：文本文件(.csv/.xlsx)、图片(.png)、文档(.doc)
# 支持指定收件人(receivers，列表)和抄送人(acc，列表)
# """

import base64
import os
import smtplib
from email.mime.text import MIMEText
from email.header import Header
from email.mime.multipart import MIMEMultipart

def my_email(user, password, receivers, subject, text, acc = None, file_path = None):
    '''
    可发送普通邮件和带附件的邮件，附件格式有：文本文件、图片、文档
    :param user: 发件人邮箱，密文
    :param password: 发件人密码，密文
    :param receivers: 接收人邮箱，传入一个列表
    :param subject: 邮件标题，字符串
    :param text: 邮件正文，字符串
    :param acc: 抄送人，传入一个列表，默认为空
    :param file_path: 附件路径，列表，默认为空
    :return: 发送邮件
    '''
    # 密文转化，所使用的服务器和端口写死
    decode = lambda string: base64.b64decode(string).decode()
    mail_host = ""  # 服务器密文
    mail_port = ''  # 端号密文
    mail_user = decode(user)  # 用户名
    mail_pass = decode(password)  # 口令
    mail_host = decode(mail_host)  # 设置服务器
    mail_port = int(decode(mail_port))  # 端口号

    print('开始发送邮件~')
    print('发送给：', receivers)
    print('抄送给：', acc)
    #创建实例
    message = MIMEMultipart()
    # 发送人和接收人
    sender = mail_user
    message['From'] = sender
    message['To'] = ",".join(receivers)

    # 标题和正文
    message['Subject'] = Header(subject , 'utf-8')
    message.attach(MIMEText(text, 'plain', 'utf-8'))

    # 若有抄送人，则添加抄送人
    if acc :
        print('有抄送人')
        message['Cc'] = ",".join(acc)
    else:
        acc=[]

    # 区分有附件和无附件
    if file_path:
        print('需要发送的附件为：')
        for file in file_path:
            filename = os.path.basename(file)
            print(filename)
            # 构造附件
            j = MIMEText(open(file, 'rb').read(), 'base64', 'utf-8') # 附件可为文本文件、照片和文档
            j["Content-Type"] = 'application/octet-stream' # 创建附件对象
            j.add_header('Content-Disposition', 'attachment',filename=('gbk', '', filename)) # 给附件添加头文件
            message.attach(j) # 将附件附加到根容器
    else:
        print('无附件')

    # 发送邮件，包括登录和发送
    try:
        smtpObj = smtplib.SMTP()
        smtpObj.connect(mail_host, mail_port)  # 25 为 SMTP 端口号
        smtpObj.login(mail_user, mail_pass)
        print("登录成功！")
        smtpObj.sendmail(sender, receivers + acc, message.as_string())
        print("恭喜，邮件发送成功！")
    except smtplib.SMTPException:
        print("Error: 无法发送邮件！")


def test():
    '''
    用于测试
    :return: 
    '''
    user = ""  # 公共邮箱用户名
    password = ""  # 口令
    receivers = ['', ''] # 接收人
    subject = '标题/主题'
    text = '正文'
    file_path = ['评估.xlsx']
    acc = [] # 抄送人
    my_email(user,password,receivers,subject,text,acc = acc,file_path = file_path)

if __name__ =='__main__':
    test()





























# #邮件的模块
# from email.header import Header
# from email.mime.text import MIMEText
# from email.utils import parseaddr, formataddr
# import smtplib
# import base64
# from aipurchase.config.config import email_params
#
# def conn_server():
#     '''连接腾讯的邮件服务器,返回连接对象.
#     参数：公司邮箱的账号和密码
#     '''
#     # 使用加密方式的端口是 465，不加密的端口是 25
#     # 在这里引用登录账号信息
#
#     user,pwd,smtp,port=email_params
#     decode = lambda string: base64.b64decode(string).decode()
#     user, pwd, smtp, port = decode(user), decode(pwd), decode(smtp),decode(port)
#     server = smtplib.SMTP(smtp, int(port))
#     server.set_debuglevel(1)
#     server.login(user, pwd)
#     return server
#
# def send_me_email(to_addr=[], header="", messages=""):
#     '''发送emial，默认是发送文本Email。
#     参数，邮箱的连接参数，接收人地址(list格式)，邮件标题，和正文。'''
#     # 创建连接
#     server=conn_server()
#     # 实例化一个msg，只有实例化msg后才能设置主题
#     msg = MIMEText(messages, 'plain', 'utf-8')  # 默认发送文本Email（plain）
#     # 设置主题
#     msg['From'] = server.user  # 发件人
#     msg['To'] = ",".join(to_addr)  # 收件人
#     msg['Subject'] = header  # 标题
#     # 发送email
#     server.sendmail(server.user, to_addr, msg.as_string())
#     # 发送后关闭连接
#     server.quit()
#
#
#
#
#
# def test():
#     send_me_email(to_addr=['suzhenyu@banggood.com'],
#                   header='标题', messages='test')