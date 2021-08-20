# -*- coding: UTF-8 -*-
"""
Author: Sunck
note: 对于数据库的操作方法
Created time: 2021/06/28
"""
import time
import json
import random
import pymysql
import platform
import configparser
from redis.sentinel import Sentinel
from ..logger import LOG

config = configparser.ConfigParser()
config.read('./conf/conf.ini')  # 读取配置文件
mysql_host = config.get('MySQL', 'ip')  # mysql的地址
redis_db = int(config.get('Redis', 'redis_db'))  # 使用redis的库
redis_password = config.get('Redis', 'redis_password')  # redis服务的密码
expire_time = config.get('Redis', 'expire_time')  # 代理ip在redis中的存活时间
mysql_password = config.get('MySQL', 'mysql_password')  # mysql的连接密码


def crawler_jd_con(db_name):
    """
    获取数据库的连接
    :param db_name: 数据库名称
    :return: 连接的游标
    """
    os = platform.system()
    # 判断操作系统类别,便于程序的转移程序
    if os == "Windows":
        host = "36.133.93.100"
    elif os == "Linux":
        host = "192.168.100.7"
    conn = pymysql.connect(host=host, port=3306, user="root",
                           password=mysql_password, database=db_name,
                           charset="utf8mb4")
    cursor = conn.cursor()
    return cursor, conn


def python_sql_mysql(db_name, sql, is_return=False):
    """
   接受查询语句,执行相对应的sql代码,判断是否需要返回
   :param db_name: 数据库的名称
   :param sql: 需要执行的sql语句
   :param is_return: 此函数是否需要返回值的条件,默认为False
   :return: 如果需要返回值则返回查询结果,如果不需要返回值,则不返回
   """
    cursor, conn = crawler_jd_con(db_name=db_name)  # 获取连接游标
    cursor.execute(sql)  # 执行sql语句
    # 判断是否返回,如果为True对齐进行查询操作,如果为False则进行插入操作
    if is_return:
        res = cursor.fetchall()
        cursor.close()
        conn.close()
        return res
    else:
        conn.commit()
        cursor.close()
        conn.close()


def is_get_redis_proxy_ip(proxy_ip='', is_return=False):
    """
    代理ip在redis中的操作,is_return=False代表不用返回,只进行存储服务,
    is_return=True代表需要返回值,默认是不返回状态
    :param proxy_ip: 代理ip的值
    :param is_return: 判断是否返回代理ip
    :return: 返回redis中存活的代理ip值或者不返回
    """

    # 判断是存服务还是取服务,如果为真则为取服务
    if is_return:
        while True:
            try:
                # redis哨兵的ip及个字占用的端口号
                sentinel = Sentinel(
                    [('192.168.100.7', 26379), ('192.168.100.24', 26379),
                     ('192.168.100.20', 26379), ], socket_timeout=0.5)
                # 获取一个从服务,负责读redis中的代理ip值
                slave = sentinel.slave_for('mymaster', socket_timeout=0.5,
                                           password=redis_password,
                                           db=redis_db)
                # 将redis中存在的所有键值生成一个列表
                redis_key_list = [int(i) for i in slave.keys()]
                print('ip池剩余', redis_key_list)
                # 当代理ip列表有值时
                if redis_key_list:
                    ip_id = random.choice(redis_key_list)  # 随机挑选一个代理ip的键值
                    print('取出的ip是', slave.get(ip_id), '存活时间剩余',
                          slave.ttl(ip_id))
                    # 获取此键值对应的代理ip值
                    ip = str(slave.get(ip_id)).split('b\'')[1].split('\'')[0]
                    ip_dict = json.loads(ip)  # 直接转成字典类型
                    break
                else:
                    print('ip池为空')
                    time.sleep(10)
            # 错误类型可能是redis连接问题,也可能出现在读取和使用时间产生差异
            except (TimeoutError, IndexError):
                LOG.warning('获取redis中的代理ip出现超时或者索引错误')
            except:
                LOG.warning('获取redis中的代理ip出现第三方错误')
            continue
        return ip_dict
    else:
        while True:
            try:
                # redis哨兵的ip及个字占用的端口号
                sentinel = Sentinel(
                    [('192.168.100.7', 26379), ('192.168.100.24', 26379),
                     ('192.168.100.20', 26379), ], socket_timeout=0.5)
                # 获取redis主服务,用于写入代理ip值
                master = sentinel.master_for('mymaster', socket_timeout=0.5,
                                             password=redis_password,
                                             db=redis_db)
                # 获取redis中所有key值组成的列表
                redis_key_list = [i for i in master.keys()]
                # 为此ip分配一个随机的key值,因为过期时间只有90秒,
                # 所以redis中键值对的数量可以维持在10**4之内
                id = random.randint(1000, 9999)
                if id in redis_key_list:
                    print('键值存在')
                    pass
                else:
                    break
            # 错误类型可能是redis连接问题,也可能出现在读取和使用时间产生差异
            except (TimeoutError, IndexError):
                LOG.warning('获取redis中的代理ip出现超时或者索引错误')
            except:
                LOG.warning('获取redis中的代理ip出现第三方错误')
            continue
        print(id)
        master.set(id, proxy_ip)  # 设置值
        master.expire(id, expire_time)  # 设置存活时间
        print('设置成功')
