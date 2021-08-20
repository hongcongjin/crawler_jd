# -*- coding: UTF-8 -*-
# ！/root/.virtualenvs/test/bin/python3.7
"""
Author: Sunck
note: 用于爬取京东商品的优惠劵信息
Created time: 2021/8/10 11:23
"""
import re
import time
import json
import random
import pymysql
import platform
import requests
import numpy as np
from redis.sentinel import Sentinel
from pymysql.converters import escape_string
from ..logger import LOG

# 市场主流请求头列表
user_agent_pc = [  # 谷歌
    'Mozilla/5.0.html (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.html.2171.71 Safari/537.36',
    'Mozilla/5.0.html (X11; Linux x86_64) AppleWebKit/537.11 (KHTML, like Gecko) Chrome/23.0.html.1271.64 Safari/537.11',
    'Mozilla/5.0.html (Windows; U; Windows NT 6.1; en-US) AppleWebKit/534.16 (KHTML, like Gecko) Chrome/10.0.html.648.133 Safari/534.16',
    # 火狐
    'Mozilla/5.0.html (Windows NT 6.1; WOW64; rv:34.0.html) Gecko/20100101 Firefox/34.0.html',
    'Mozilla/5.0.html (X11; U; Linux x86_64; zh-CN; rv:1.9.2.10) Gecko/20100922 Ubuntu/10.10 (maverick) Firefox/3.6.10',
    # opera
    'Mozilla/5.0.html (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/39.0.html.2171.95 Safari/537.36 OPR/26.0.html.1656.60',
    # qq浏览器
    'Mozilla/5.0.html (compatible; MSIE 9.0.html; Windows NT 6.1; WOW64; Trident/5.0.html; SLCC2; .NET CLR 2.0.html.50727; .NET CLR 3.5.30729; .NET CLR 3.0.html.30729; Media Center PC 6.0.html; .NET4.0C; .NET4.0E; QQBrowser/7.0.html.3698.400)',
    # 搜狗浏览器
    'Mozilla/5.0.html (Windows NT 5.1) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.html.963.84 Safari/535.11 SE 2.X MetaSr 1.0.html',
    # 360浏览器
    'Mozilla/5.0.html (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/30.0.html.1599.101 Safari/537.36',
    'Mozilla/5.0.html (Windows NT 6.1; WOW64; Trident/7.0.html; rv:11.0.html) like Gecko',
    # uc浏览器
    'Mozilla/5.0.html (Windows NT 6.1; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/38.0.html.2125.122 UBrowser/4.0.html.3214.0.html Safari/537.36',
    "Mozilla/5.0 (Macintosh; U; Intel Mac OS X 10_6_8; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50",
    "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-us) AppleWebKit/534.50 (KHTML, like Gecko) Version/5.1 Safari/534.50",
    "Mozilla/5.0 (Windows NT 10.0; WOW64; rv:38.0) Gecko/20100101 Firefox/38.0",
    "Mozilla/5.0 (Windows NT 10.0; WOW64; Trident/7.0; .NET4.0C; .NET4.0E; .NET CLR 2.0.50727; .NET CLR 3.0.30729; .NET CLR 3.5.30729; InfoPath.3; rv:11.0) like Gecko",
    "Mozilla/5.0 (compatible; MSIE 9.0; Windows NT 6.1; Trident/5.0)",
    "Mozilla/4.0 (compatible; MSIE 8.0; Windows NT 6.0; Trident/4.0)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 6.0)",
    "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10.6; rv:2.0.1) Gecko/20100101 Firefox/4.0.1",
    "Mozilla/5.0 (Windows NT 6.1; rv:2.0.1) Gecko/20100101 Firefox/4.0.1",
    "Opera/9.80 (Macintosh; Intel Mac OS X 10.6.8; U; en) Presto/2.8.131 Version/11.11",
    "Opera/9.80 (Windows NT 6.1; U; en) Presto/2.8.131 Version/11.11",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_7_0) AppleWebKit/535.11 (KHTML, like Gecko) Chrome/17.0.963.56 Safari/535.11",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Maxthon 2.0)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; TencentTraveler 4.0)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; The World)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Trident/4.0; SE 2.X MetaSr 1.0; SE 2.X MetaSr 1.0; .NET CLR 2.0.50727; SE 2.X MetaSr 1.0)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; 360SE)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1; Avant Browser)",
    "Mozilla/4.0 (compatible; MSIE 7.0; Windows NT 5.1)", ]


def get_redis_ip(redis_db, redis_password):
    """
    从redis中随机取出一个存活的ip返回
    :param redis_db: 使用的redis库
    :param redis_password: redis的密码
    :return: 存活的代理ip值
    """
    while True:
        try:
            sentinel = Sentinel(
                [('192.168.100.7', 26379), ('192.168.100.24', 26379),
                 ('192.168.100.20', 26379), ], socket_timeout=0.5)
            # 通过哨兵获取一个redisSlave
            slave = sentinel.slave_for('mymaster', socket_timeout=0.5,
                                       password=redis_password, db=redis_db)
            # 获取由代理键值队列,每一个键值对应一个代理ip值
            redis_key_list = [int(i) for i in slave.keys()]
            if redis_key_list:
                ip_id = random.choice(redis_key_list)
                ip = str(slave.get(ip_id)).split('b\'')[1].split('\'')[0]
                ip_dict = json.loads(ip)  # 直接转成字典类型
                break
            else:
                # 可能出现代理ip池为空的状态
                time.sleep(10)
        except (TimeoutError,):
            LOG.warning('连接超时')
            continue
        except (IndexError,):
            LOG.warning('索引错误')
            continue
        except:
            LOG.warning('第三方错误')
            continue
    return ip_dict


def crawler_jd_con(db_name):
    """
    获取数据库的连接
    :param db_name: 数据库名称
    :return: 连接的游标
    """
    os = platform.system()
    # 判断操作系统类别,便于程序的转移程序
    if os == "Windows":
        mysql_host = "36.133.93.100"
    elif os == "Linux":
        mysql_host = "192.168.100.7"
    conn = pymysql.connect(host=mysql_host, port=3306, user="root",
                           password='mysql5@Crm..', database=db_name,
                           charset="utf8mb4")
    cursor = conn.cursor()
    return cursor, conn


def get_content_ua(db_name):
    """
    将存储在mysql的cookie数据进行组转
    :param db_name: cookie存储的数据库名字
    :return: 组装好的cookie字典和可选cookie列表
    """
    while True:
        select_sql = "select purpose_value from request_header where " \
                     "purpose_key='comment_condition'"
        is_return = python_sql_mysql(sql=select_sql, is_return=True,
                                     db_name=db_name)
        # 判断是否返回数据库中的值
        if is_return:
            if is_return[0][0] == '1':
                break
            else:
                print('在更换变量,稍等')
                time.sleep(10)
    select_sql = "select purpose_value from request_header where " \
                 "purpose_key='comment_cookie'"
    comment_ua_id = \
        python_sql_mysql(sql=select_sql, db_name=db_name, is_return=True)[0]
    comment_ua_id_list = [int(i) for i in comment_ua_id[0].split(',')]
    select_sql = "select id,purpose_value from request_header " \
                 "where purpose_key='get_json_user_agent_pc'"
    comment_ua = python_sql_mysql(sql=select_sql, db_name=db_name,
                                  is_return=True)
    comment_ua_dict = {id: ua for id, ua in comment_ua}
    return comment_ua_id_list, comment_ua_dict


def get_json_user_agent_pc(db_name):
    """
    去数据库中取出cookie值,实现请求头组装,随机返回请求json数据的请求头
    :return: 返回请求json数据的请求头
    """
    content_ua_id_list, content_ua_dict = get_content_ua(db_name=db_name)
    ua_id = random.choice(content_ua_id_list)
    if ua_id == 1:
        json_user_agent = {'user-agent': random.choice(user_agent_pc),
                           'referer': 'https://item.jd.com/'}
    else:
        json_user_agent = {'user-agent': random.choice(user_agent_pc),
                           'referer': 'https://item.jd.com/',
                           'Cookie': content_ua_dict.get(ua_id)}
    return json_user_agent


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


def get_proxy():
    """
    调用代理ip接口,获取代理ip值,并且将这些代理ip值和京东码进行关联存储
    :return: 返回代理ip的值
    """
    while True:
        try:
            url = "http://gec.ip3366.net/api/?key=20210524224808126&getnum=1&anonymoustype=3&filter=1&area=1&formats=2"
            response = requests.get(url=url, timeout=5)
            data_json = response.text
            ip = re.findall(r'"Ip":"(.*?)",', data_json)[0]  # 获取到的ip
            port = re.findall(r'"Port":(.*?),', data_json)[0]  # 获取到的端口号
            proxy = {'http': ip + ':' + port, }
        except:
            time.sleep(10)
            print('调用代理ip错误')
            continue
        break
    return proxy


def get_jd_coupon(jd_product_id, **kwargs):
    """
    由商品的京东码来抓取对应商品的优惠劵信息,并且入库
    :param jd_product_id: 京东码
    :param kwargs: 参数字典
    :return: 不返回
    """
    jquery_list = ['jQuery8195764', 'jQuery433168', 'jQuery3481898',
                   'jQuery7366219', 'jQuery2166058', 'jQuery1219778',
                   'jQuery5849685', 'jQuery2209741', 'jQuery6788690',
                   'jQuery1164629', 'jQuery268964', 'jQuery60405',
                   'jQuery4339096', 'jQuery3328205', 'jQuery35752']
    jquery = random.choice(jquery_list)
    select_sql = "select jd_product_id,shop_id,vender_id,cat_id,param_json " \
                 "from jd_product_coupon where jd_product_id={}".format(
        jd_product_id)
    rest = python_sql_mysql(sql=select_sql, db_name=kwargs.get('temp_db_name'),
                            is_return=True)
    jd_product_id = jd_product_id
    shop_id = rest[0][1]
    cat = rest[0][3]
    param_json = rest[0][4]
    if shop_id in [0, -1]:
        pass
        print('shop_id为0,我无法处理')
    else:
        # python requests 可以直接发送带{}请求数据。
        url = 'https://item-soa.jd.com/getWareBusiness'
        payload = {'callback': '{}'.format(jquery),
                   'skuId': '{}'.format(str(jd_product_id)),
                   'cat': '{}'.format(cat), 'area': '6_303_304_0',
                   'shopId': '{}'.format(shop_id), 'venderId': '1000075604',
                   'paramJson': '{}'.format(param_json), 'num': '1'}
        while True:
            try:
                # proxy = get_proxy() #在出现紧急情况时候可以直接调用代理ip接口
                proxy = get_redis_ip(redis_db=kwargs.get('redis_db'),
                                     redis_password=kwargs.get(
                                         'redis_password'))
                response = requests.get(url=url, params=payload,
                                        headers=get_json_user_agent_pc(
                                            kwargs.get('temp_db_name')),
                                        proxies=proxy, timeout=5)
            except:
                return True
            try:
                response_contents = re.findall(r'{}[(](.*)[)]'.format(jquery),
                                               response.text)
                response_content_json = json.loads(response_contents[0])
            except:
                time.sleep(random.uniform(10, 15))
                continue
            break
        raw_json = json.dumps(response_content_json, ensure_ascii=False)
        # 原始返回的json数据存入数据库中
        insert_sql = "update jd_product_coupon set raw_json=\'%s\' " \
                     "where jd_product_id=%s" % (
                         escape_string(raw_json), jd_product_id)
        python_sql_mysql(db_name=kwargs.get('temp_db_name'), sql=insert_sql)
        coup_dict = {}  # 促销信息字典
        for act in response_content_json.get('promotion').get('activity'):
            if act.get('text'):
                if act.get('text') in coup_dict:
                    temp = coup_dict.get(act.get('text')) + '；' + act.get(
                        'value')
                    coup_dict.update({act.get('text'): temp})
                else:
                    coup_dict.update({act.get('text'): act.get('value')})
            else:
                if '新人优惠活动' in coup_dict:
                    pass
                else:
                    coup_dict.update({'新人优惠活动': act.get('value')})
                    print('新人优惠活动', act.get('value'))
        # 将促销信息加入数据库
        if coup_dict == {}:
            pass
        else:
            coup_json = json.dumps(coup_dict, ensure_ascii=False)
            insert_sql = "update jd_product_coupon set " \
                         "promotion_sale=\'%s\' where jd_product_id=%s" % (
                             escape_string(coup_json), jd_product_id)
            python_sql_mysql(db_name=kwargs.get('temp_db_name'),
                             sql=insert_sql)
        # 优惠劵信息列表
        promotion_sale_list = []
        for coup in response_content_json.get('couponInfo'):
            print(coup.get('discountText'))
            promotion_sale_list.append(coup.get('discountText'))
        # 将每个优惠劵信息通过中文的分号相隔,合并成一个大的字符串存入数据库
        promotion_sale_str = '；'.join(promotion_sale_list)
        print(promotion_sale_str)
        if promotion_sale_str == '':
            pass
        else:
            # 将优惠劵信息加入数据库
            insert_sql = "update jd_product_coupon set coupon=\'%s\' " \
                         "where jd_product_id=%s" % (
                             escape_string(promotion_sale_str), jd_product_id)
            python_sql_mysql(db_name=kwargs.get('temp_db_name'),
                             sql=insert_sql)
        time.sleep(random.uniform(6, 7))


if __name__ == '__main__':
    redis_db = 1
    redis_password = 'redis5@Crm..'
    temp_db_name = 'crawler_jd_assist'
    info_dict = {'redis_db': redis_db, 'redis_password': redis_password,
                 'temp_db_name': temp_db_name}
    select_sql = "select jd_product_id from jd_product_coupon where raw_json is null"
    exist_np_1 = np.array([i[0] for i in python_sql_mysql(is_return=True,
                                                          db_name=temp_db_name,
                                                          sql=select_sql)])
    for jd_product_id in exist_np_1:
        get_jd_coupon(jd_product_id=jd_product_id, **info_dict)
