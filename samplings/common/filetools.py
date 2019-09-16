#!/usr/bin/python3
# -*- coding: UTF-8 -*-

import logging
import os

logger = logging.getLogger(__name__)


def read(file_path, func=None, encoding='utf-8'):
    if os.path.isdir(file_path):
        raise FileNotFoundError('{} is directory -> ignore it' % file_path)

    lines = []
    with open(file_path, 'r', encoding=encoding) as f:
        for line in f.readlines():
            line = line.strip('\n')
            # logger.debug(line)
            lines.append(func(line) if func else line)

    logger.debug("===== read end. file path: %s, lines: %d", file_path, len(lines))
    return lines


def write(file_path, body, mode='w', encoding='utf-8'):
    '''
    > mode='w'
    打开一个文件只用于写入。如果该文件已存在则打开文件，并从开头开始编辑，即原有内容会被删除。
    如果该文件不存在，创建新文件。\n
    > mode='a'
    打开一个文件用于追加。如果该文件已存在，文件指针将会放在文件的结尾。也就是说，新的内容将会被写入到已有内容之后。
    如果该文件不存在，创建新文件进行写入。
    > 缓冲
    在文件关闭前 file.close() 或缓冲区刷新前 file.flush()，
    字符串内容存储在缓冲区中，这时你在文件中是看不到写入的内容的。
    '''
    if os.path.isdir(file_path):
        raise FileNotFoundError('{} is directory -> ignore it' % file_path)

    with open(file_path, mode, encoding=encoding) as f:
        length = f.write(body)

    logger.debug("===== write end. file path: %s, length: %d", file_path, length)
