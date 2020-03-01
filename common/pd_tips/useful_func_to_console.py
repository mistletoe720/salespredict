# -*- coding: utf-8 -*-
"""
控制鼠标键盘，将代码发送到console，而不是一条条复制粘贴，提交效率

使用方法：

将pycharm 放在屏幕的左边，xshell放在右边
选中一段代码，发送到console，会模拟鼠标键盘复制到xshell中

"""
import time
import re
import pyperclip
from pynput.mouse import Button, Controller


def add_new_line(string):
    tmp = ''
    for line in string.split('\n\n'):
        if line == '':
            continue
        else:
            # 如果仅有一行
            if line.count('\n') == 0:
                tmp = tmp + '\n' + line
                continue
            else:
                tmp = tmp + '\n\n' + line + '\n'
    # print(tmp)
    return tmp


def new_code(string):
    # 去掉所有空行
    p1 = '\s+?\n'
    p11 = re.compile(p1)
    string = p11.sub('\n', string)
    # 添加空行
    p2 = '\n(?=\S)'
    p22 = re.compile(p2)
    string = p22.sub('\n\n', string)
    # 如果上下两行是并行的，删除第二步添加的空行
    string = add_new_line(string)

    # print(string)
    return string + '\n'


def mouse_click(mouse):
    # 获取原鼠标位置
    x, y = mouse.position
    mouse.position = (1740, 700)  # 移动鼠标,根据实际修改-----------------
    mouse.press(Button.left)  # 单击激活窗口
    mouse.release(Button.left)
    mouse.press(Button.right)  # 右击粘贴内容
    mouse.release(Button.right)
    time.sleep(0.5)  # 一定要暂停一小会
    mouse.position = (x, y)  # 恢复鼠标位置
    mouse.press(Button.left)  # 单击
    mouse.release(Button.left)


def to_console():
    while True:
        string = input()
        if string == 'break':
            break
        string = new_code(string)
        pyperclip.copy(string)
        mouse = Controller()
        mouse_click(mouse)


if __name__ == '__main__':
    # 运行下面的函数开始工作
    to_console()
