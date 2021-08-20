# -*- coding: UTF-8 -*-
# ！/root/.virtualenvs/test/bin/python3.7
"""
Author: Sunck
note: 用于爬取京东商品爬取的主函数
Created time: 2021/06/28
"""
import re
import json
import time
import queue
import random
import requests
import threading
import numpy as np
import configparser
from lxml import html

etree = html.etree
from pymysql.converters import escape_string
from settings_package.jd_xpath import jd_xpath_main
from settings_package.data_sort import data_sort_main
from settings_package.db_function import is_get_redis_proxy_ip, \
    python_sql_mysql
from settings_package.crawler_robot import send_msg
from settings_package.crawler_header import get_product_html_user_agent_pc
from .logger import LOG


def page_detail(jd_product_id, proxy, **kwargs):
    """
    对于商品页面进行抓取,并且将抓取的页面数据进行解析
    :param jd_product_id: 京东码
    :param proxy: 代理ip信息
    :return: 如果成功抓取返回True,如果没有成功抓取返回False
    """
    # 计数器,当京东搜索引擎五次无法搜索出相对应的页面,程序自动判定为该商品被下架
    count = 0
    while True:
        try:
            response = requests.get(
                url='https://item.jd.com/{}.html'.format(jd_product_id),
                headers=get_product_html_user_agent_pc(), proxies=proxy,
                timeout=5)
        except:
            print('请求页面出错')
            continue
        print(response.status_code)
        # 将返回的html页面解析成一个树,便于后期的信息提取
        html1 = etree.HTML(response.text)
        # 直接将页面返回数据进行解析
        product_intr = html1.xpath(
            '//*[@id="detail"]/div[2]/div[1]/div[1]')  # 商品介绍模块
        # 商品主体模块
        product_body = html1.xpath('//*[@id="detail"]/div[2]/div[2]')
        print(product_intr, product_body)
        # 判定在返回的页面中是否有有介绍模块和主体模块,如果有,则继续解析,如果没有,则重复请求页面
        if not product_intr == [] and not product_body == []:
            break
        else:
            if count <= 5:  # 商品是否下架的判定条件
                time.sleep(3)
                count += 1  # 判定是否下架的约束条件
            else:
                return False
    # 原始html数据存入mysql数据库
    select_sql = "select jd_product_id from jd_raw_html where " \
                 "jd_product_id=%s" % (jd_product_id)
    if python_sql_mysql(db_name=kwargs.get('db_name'), sql=select_sql,
                        is_return=True):
        pass
    else:
        insert_sql = "insert into jd_raw_html (jd_product_id,jd_raw_html) " \
                     "values (%s,\'%s\')" % (
                         jd_product_id, escape_string(response.text))
        python_sql_mysql(db_name=kwargs.get('db_name'), sql=insert_sql)
    # 从xpath_main中获取解析好的数据包
    all_dict = jd_xpath_main(response=response, proxy=proxy,
                             jd_product_id=jd_product_id,
                             comment_page=kwargs.get('comment_page'),
                             question_page=kwargs.get('question_page'))
    # 考虑到可能会返回False情况,当返回为False时,代表此商品非正常的京东商品信息
    if not all_dict:
        return False
    else:
        # 原始json数据存入mysql数据库
        all_json = json.dumps(all_dict, ensure_ascii=False)
        select_sql = "select jd_product_id from jd_raw_json where " \
                     "jd_product_id=%s" % (jd_product_id)
        if python_sql_mysql(db_name=kwargs.get('db_name'), sql=select_sql,
                            is_return=True):
            pass
        else:
            # 使用不转义的语法插入数据库中
            insert_sql = "insert into jd_raw_json (jd_product_id,jd_raw_json) " \
                         "values (%s,\'%s\')" % (
                             jd_product_id, escape_string(all_json))
            python_sql_mysql(db_name=kwargs.get('db_name'), sql=insert_sql)
        # 将产生的数据包传入队列中,交付给两个子线程处理
        q_dict.put({jd_product_id: all_dict})
        # 每个京东商品爬取的间隔时间为2秒,人为降低访问速度
        time.sleep(2)


def detail_dict():
    """
    子线程处理函数
    :return: 不返回
    """
    while True:
        # 判断队列是否为空,如果队列中有数据包,则将数据包取出进行处理
        if not q_dict.empty():
            detail_dict = q_dict.get()  # 不为空的时候取到数据
            if detail_dict.keys() and detail_dict.values():  # 对于数据进行解析
                jd_product_id = list(detail_dict.keys())[0]
                crawler_dict = list(detail_dict.values())[0]
                jd_product_id = int(jd_product_id)
                try:
                    data_sort_main(jd_product_id=jd_product_id,
                                   crawler_dict=crawler_dict, db_name=db_name)
                    print('执行成功')
                # 当商品入库时发生错误时记录异常数据
                except:
                    LOG.warning(str(jd_product_id) + '发生入库失败')
                pass


def run2(threading_number):
    """
    创建两个子线程去处理主线程产生的数据包
    :param threading_number: 线程数量
    :return: 不返回
    """
    for i in range(threading_number):
        p = threading.Thread(target=detail_dict)
        p.start()


def get_proxy(jd_product_id):
    """
    调用代理ip接口,获取代理ip值,并且将这些代理ip值和京东码进行关联存储
    :param jd_product_id: 京东码
    :return: 返回代理ip的值
    """
    while True:
        try:
            url = "http://gec.ip3366.net/api/?key=20210524224808126&getnum=1&anonymoustype=3&filter=1&area=1&formats=2"
            resp = requests.get(url=url, timeout=5)
            data_json = resp.text
            ip = re.findall(r'"Ip":"(.*?)",', data_json)[0]  # 获取到的ip
            port = re.findall(r'"Port":(.*?),', data_json)[0]  # 获取到的端口号
            proxy = {'http': ip + ':' + port, }
        except:
            time.sleep(10)
            print('调用代理ip错误')
            continue
        break
    print(proxy)
    proxy_str = json.dumps(proxy)  # 将字典值转成字符串
    insert_sql = "insert into proxy_ip (jd_product_id,proxy_ip) value " \
                 "(%s,\'%s\')" % (jd_product_id, proxy_str)
    python_sql_mysql(sql=insert_sql, db_name=assist_db_name_1)
    return proxy


def main(**kwargs):
    """
    爬虫的主函数
    :param kwargs: 参数字典
    :return: 不返回
    """
    # 查询待爬取,已经匹配好的京东商品数据
    select_sql = "select DISTINCT jd_product_id from %s" % (
        kwargs.get('table_name'))
    # 将生成的列表数据转成array
    # 所有需要抓取的商品信息数据
    exist_np_1 = np.array([i[0] for i in python_sql_mysql(is_return=True,
                                                          db_name=kwargs.get(
                                                              'assist_db_name'),
                                                          sql=select_sql)])
    # 查询已经爬取的商品数据
    select_sql = 'select DISTINCT jd_product_id from jd_data_product_info'
    success_np_2 = np.array([i[0] for i in python_sql_mysql(is_return=True,
                                                            db_name=kwargs.get(
                                                                'db_name'),
                                                            sql=select_sql)])
    # 查询匹配错误的商品数据
    select_sql = 'select DISTINCT jd_product_id from error_jd_product'
    error_np_3 = np.array([i[0] for i in python_sql_mysql(is_return=True,
                                                          db_name=kwargs.get(
                                                              'assist_db_name'),
                                                          sql=select_sql)])
    # 生成的rests是已经匹配,没有被爬取,也不存在与错误商品数据中的京东码
    rests = [i for i in exist_np_1 if
             i not in success_np_2 and i not in error_np_3]
    # 启动两个子线程用于存储抓取下来的数据,后期随着数据量的增加,线程数量可以变化
    run2(threading_number=kwargs.get('threading_number'))
    for rest in rests:
        jd_product_id = rest
        select_sql = "select jd_product_id from jd_data_product_info " \
                     "where jd_product_id=%s" % (jd_product_id)
        if python_sql_mysql(db_name=kwargs.get('db_name'), sql=select_sql,
                            is_return=True):
            print('商品存在')
            continue
        select_sql = 'select * from error_jd_product where ' \
                     'jd_product_id=%s' % (jd_product_id)
        if python_sql_mysql(db_name=kwargs.get('assist_db_name'),
                            sql=select_sql, is_return=True):
            print('商品存在')
            continue
        else:
            try:
                proxy = get_proxy(jd_product_id)  # 进入处理函数
                # 如果返回的是False,代表京东码有错误,没有获得相应页面
                if not page_detail(jd_product_id, proxy, **kwargs):
                    # 如果五次请求无法取得相对应的页面,程序默认请求失败,插入失败数据库
                    select_sql = 'select * from error_jd_product where ' \
                                 'jd_product_id=%s' % (jd_product_id)
                    if python_sql_mysql(db_name=kwargs.get('assist_db_name'),
                                        sql=select_sql, is_return=True):
                        pass
                    else:
                        insert_sql = "insert into error_jd_product " \
                                     "(jd_product_id) VALUES (%s)" % (
                                         jd_product_id)
                        python_sql_mysql(db_name=kwargs.get('assist_db_name'),
                                         sql=insert_sql)
                    continue
                else:  # 如果成功,则将代理ip插入redis中
                    proxy_str = json.dumps(proxy)  # 将字典值转成字符串
                    # 将产生的代理ip数据传入redis中存储
                    is_get_redis_proxy_ip(proxy_ip=proxy_str)
                    print('插入代理ip池成功')
            except:
                send_msg(jd_product_id)  # 发出警告
                LOG.warning(str(jd_product_id) + '主爬虫出现错误')
            finally:
                time.sleep(random.uniform(1, 2))
    print('遍历完成')


def test_main(jd_product_id, **kwargs):
    """
    这是一个对于某个制定商品的爬取函数,作用是针对抛异常,或者是警告的一小部分商品实现手动爬取
    :param jd_product_id: 京东码
    :param kwargs: 参数字典
    :return: 不返回
    """
    run2(threading_number=threading_number)
    select_sql = "select jd_product_id from jd_data_product_info " \
                 "where jd_product_id=%s" % (jd_product_id)
    if python_sql_mysql(db_name=kwargs.get('db_name'), sql=select_sql,
                        is_return=True):
        print('商品存在')
    else:
        # 因为此函数只是对个别商品进行爬取,所以代理ip方面可要可不要,完全按照自己意愿
        # proxy = get_proxy()
        proxy = get_proxy(jd_product_id)
        page_detail(str(jd_product_id), proxy=proxy, **kwargs)


if __name__ == '__main__':
    q_dict = queue.Queue()  # 创建线程调度队列,将主线程产生的任务包
    config = configparser.ConfigParser()
    config.read('./conf/conf.ini')
    # 启动的存储线程数量
    threading_number = int(config.get('Crawler_jd', 'threading_number'))
    # 爬取评论的页数
    comment_page = int(config.get('Crawler_jd', 'comment_page'))
    # 爬取问答的页数
    question_page = int(config.get('Crawler_jd', 'question_page'))
    # 操作的数据库名称
    db_name = config.get('MySQL', 'db_name')
    # 辅助数据库名称
    assist_db_name = config.get('MySQL', 'assist_db_name')
    # 辅助数据库名称
    assist_db_name_1 = config.get('MySQL', 'assist_db_name_1')
    table_name = config.get('MySQL', 'table_name')  # 需要抓取商品数据的表名
    # 参数字典
    info_dict = {'threading_number': threading_number,
                 'comment_page': comment_page, 'question_page': question_page,
                 'db_name': db_name, 'assist_db_name': assist_db_name,
                 'assist_db_name_1': assist_db_name_1,
                 'table_name': table_name}
    print(info_dict)
    # test_main(jd_product_id=12860525976,
    #           **info_dict)
    # 对于某个商品的单个爬虫程序
    main(**info_dict)  # 主爬虫
