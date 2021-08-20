# -*- coding: utf-8 -*-
"""
Author: Hanks
note: 用于创建日志
Created time: 2020/1/21 11:23
"""

import datetime
import logging
import logging.handlers
import os


def create_log(path='log', log_name="logs"):
    """
    用于创建日志
    :param path: 只输入文档目录，默认是当前文档，到时候确定一下日志在哪
    :param log_name: 日志命名
    :return: 返回一个logger实例
    """
    dt_info = datetime.datetime.now().strftime('%Y-%m-%d')
    # 创建一个logger实例
    logger = logging.getLogger(log_name)
    logging.basicConfig(filemode='w')
    logger.setLevel(logging.INFO)
    # 日志的命名 和 日志内容的格式
    dirs = os.path.join(os.path.dirname(__file__), f"{path}")
    # 如果没有这个路径就先创建
    if not os.path.exists(dirs):
        os.mkdir(dirs)
    file_name = f"{dirs}/Rec-{dt_info}.log"
    fmt = "[%(asctime)s-%(levelname)s-%(filename)s:%(lineno)d] %(message)s"
    log_format = logging.Formatter(fmt, datefmt='%H:%M:%S')

    # StreamHandler不要也行
    # sh = logging.StreamHandler()
    # sh.setFormatter(log_format)
    # logger.addHandler(sh)

    # 创建FileHandler将日志写入磁盘，TimedRotatingFileHandler控制回滚周期等
    # 同一天“D”的日志写入一个文件内，最多备份60个文件，即61天的文件
    time_handler = logging.handlers.TimedRotatingFileHandler(file_name,
                                                             when='D',
                                                             backupCount=60,
                                                             encoding="utf8")
    time_handler.setFormatter(log_format)
    logger.addHandler(time_handler)
    return logger


LOG = create_log()
if __name__ == '__main__':
    LOG.info('哈哈')
