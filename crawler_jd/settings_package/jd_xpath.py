# -*- coding: UTF-8 -*-
# ！/root/.virtualenvs/test/bin/python3.7
"""
Author: Sunck
note: 解析原始页面中的数据,返回相对应的信息
Created time: 2021/06/28
"""
import re
import json
import time
import random
import requests
from lxml import html

etree = html.etree
from html.parser import HTMLParser
from settings_package import crawler_header

# 为你推荐接口计数变量,因为每次访问京东接口都要传入一个当前时间戳加密后的字符串,所以采用没三次传入一个相同的时间戳
recommend_count = 3
# 为你推荐接口时间戳变量,每三个商品实现更新
recommend_timec = 0
# 看了又看接口计数变量,因为每次访问京东接口都要传入一个当前时间戳加密后的字符串,但是如果每次都传入当前时间戳,京东会反爬,所以采用每三次传入一个相同的时间戳
see_count = 3
# 看了又看接口时间戳变量,后续每三次将对于这个时间戳进行更新,规避京东的反爬虫措施
see_timec = 0


def product_tag_func(html1):
    """
    从html中直接解析商品的标签信息
    :param html1: 原始的html数据
    :return: 返回该商品的标签信息,如果没有标签数据,直接返回'NULL',方便后期直接对数据进行入库操作
    """
    product_tag_list = html1.xpath("//div[contains(@class, 'sku')]/img/@alt")
    if product_tag_list:
        return ' '.join(product_tag_list)
    else:
        return 'NULL'


def product_shop_name_is_self(html1):
    """
    从html中直接解析店铺名称,以及是否自营的信息
    :param html1: 原始页面的html信息
    :return: 返回解析出来的店铺名称信息,和是否自营,自营返回'True',非自营返回'False'
    """
    shop_name = html1.xpath(
        '//*[@id="crumb-wrap"]/div/div[2]/div[2]/div[1]/div/a/text()')
    if shop_name:
        print(shop_name)
        if '自营' in shop_name[0]:
            return shop_name, 'True'
        else:
            return shop_name, 'False'
    else:
        return shop_name, 'False'


def shop_star_count_func(html1):
    """
    从html中解析出店铺的星级数量
    :param html1: 原始页面的html信息
    :return: 返回解析出来的店铺星级数量
    """
    product_start_num = html1.xpath(
        '//*[@id="crumb-wrap"]/div/div[2]/div[2]/div[2]/div/div/div/div/@title')
    return product_start_num


def product_name_func(html1):
    """
    从html中直接解析商品名称信息
    :param html1: 原始的html信息
    :return: 返回解析出来的商品名称
    """
    # 以下也是一个解析商品名称的方案,第二种方案更值得推荐
    # 1.可能会出现解码出来的商品名称出现乱码状态
    # html1=str(html1)
    # comment = re.compile(r'skuMarkJson: (.*);', re.DOTALL)
    # name_conment=comment.findall(html1)[0].replace('\n', '').replace(' ', '')
    # name=re.findall(r"name:'(.*?)'", name_conment)
    # # print(name_conment)
    # print('商品名称',name[0])
    # return name[0]
    # 2.解码商品名称稳定,推荐使用
    product_name_list = html1.xpath("//div[contains(@class, 'sku')]/text()")
    product_name_base = ''.join(product_name_list)
    product_name = product_name_base.replace(' ', '').replace('\n', '')
    return product_name


def type_name(html1):
    """
    从html中直接解析商品的所属类别信息
    :param html1: 原始的html信息
    :return: 动态解析出来的商品类别信息
    """
    type_list = []
    # type_list_1是死的数据,可以进行定向抓取
    # type_list_1是页面商品的前三个类别信息,在页面中是写死的
    type_list_1 = html1.xpath('//*[@id="crumb-wrap"]/div/div[1]/div/a/text()')
    print(type_list_1)
    type_list.extend(type_list_1)
    if type_list_1 == []:  # 如果无法解析页面中的商品类别信息,则判定这个页面是非正常页面,中断程序运行
        return False
    # 第四个商品类别信息可能出现url现象,就要分两种情况进行解析
    if len(html1.xpath(
            '//*[@id="crumb-wrap"]/div/div[1]/div[7]/div/div/div[1]/a/text()')) == 1:
        type_name_2 = html1.xpath(
            '//*[@id="crumb-wrap"]/div/div[1]/div[7]/div/div/div[1]/a/text()')
    else:
        type_name_2 = html1.xpath(
            '//*[@id="crumb-wrap"]/div/div[1]/div[9]/div/div/div[1]/a/text()')
    print(type_name_2)
    type_list.extend(type_name_2)
    # bread_name_3 是一个在三种数据进行选择的结果
    if len(html1.xpath('//*[@id="crumb-wrap"]/div/div[1]/div[7]/text()')) == 1:
        type_name_3 = html1.xpath(
            '//*[@id="crumb-wrap"]/div/div[1]/div[7]/text()')
    elif len(html1.xpath(
            '//*[@id="crumb-wrap"]/div/div[1]/div[9]/text()')) == 1:
        type_name_3 = html1.xpath(
            '//*[@id="crumb-wrap"]/div/div[1]/div[9]/text()')
    elif len(html1.xpath('//*[@id="crumb-wrap"]/div/div[1]/div[11]')) == 1:
        type_name_3 = html1.xpath(
            '//*[@id="crumb-wrap"]/div/div[1]/div[11]/text()')
    else:
        type_name_3 = []
    print(type_name_3)
    type_list.extend(type_name_3)
    return type_list


def product_price(proxy, jd_product_id):
    """
    发出json请求,获取商品的价格信息
    :param proxy: 代理信息
    :param jd_product_id: 京东码
    :return: 该商品的现价和原价
    """
    url = 'https://item-soa.jd.com/getWareBusiness'
    payload = {'callback': 'jQuery7419800',
               'skuId': '{}'.format(jd_product_id), 'cat': '737,752,14421',
               'area': '6_303_304_0', 'shopId': '1000001228',
               'venderId': '1000001228',
               'paramJson': '{"platform2":"100000000001","specialAttrStr":"p0pppppppppppppppppppp","skuMarkStr":"00"}',
               'num': '1'}
    price_response = requests.get(url=url, params=payload,
                                  headers=crawler_header.get_user_agent_pc(),
                                  proxies=proxy)
    print(price_response.status_code)
    print(price_response.text)
    price_response_content = re.findall(r'jQuery7419800[(](.*)[)]',
                                        str(price_response.text))
    if len(price_response_content) > 0:
        price_response_content_json = json.loads(price_response_content[0])
        present_price = price_response_content_json.get('price').get('p')  # 现价
        former_price = price_response_content_json.get('price').get('op')  # 原价
        print(present_price, former_price)
        return present_price, former_price
    else:
        print('价格没有检索到')
        return 0, 0


def data_product_type(html1):
    """
    从html中直接获取商品的可选类别信息
    :param html1: 原始的html信息
    :return: 商品可选类别组成的字段
    """
    type_dict = {}
    class_1 = html1.xpath(
        '//*[@id="choose-attr-1"]/div[1]/text()')  # 一级选项,例如选择颜色
    if class_1:
        product_type = html1.xpath(
            '//*[@id="choose-attr-1"]/div[2]/div/a/i/text()')
        print(class_1[0].strip(), product_type)
        type_dict.update({class_1[0].strip(): product_type})
    # 二级选项,例如选择类别,一般商品只会存在两个选项,目前只做了解析两种的
    class_2 = html1.xpath('//*[@id="choose-attr-2"]/div[1]/text()')
    product_type_2 = [i.strip() for i in html1.xpath(
        '//*[@id="choose-attr-2"]/div[2]/div/a/text()')]
    if product_type_2:
        print(class_2[0].strip(), product_type_2)
        type_dict.update({class_2[0].strip(): product_type_2})
    return type_dict


def product_body_func(html1):
    """
    从html中直接获取商品的规格与包装信息
    :param html1: 原始的html数据
    :return: 以字典的形式返回规格与包装信息
    """
    product_body_dict = {}  # 一个规格与包装的字典,最后返回数据
    html1 = etree.HTML(html1)
    print(html1.xpath('//*[@class="Ptable-item"]'))  # 其他的介绍信息
    print(html1.xpath('//*[@class="package-list"]'))  # 最后的包装清单
    # 主体参数,规格参数,基本信息等等
    for x1 in html1.xpath('//*[@class="Ptable-item"]'):
        print(x1.xpath('h3/text()'))
        if not x1.xpath('h3/text()')[0] in product_body_dict:
            # 因为规格与包装信息有两个kay,一个value值,必须使用字典嵌套字典形式
            product_body_dict.update({x1.xpath('h3/text()')[0]: {}})
        print(x1.xpath('dl/dl'))
        if x1.xpath('dl/dl'):
            for x2 in x1.xpath('dl/dl'):
                print(x2.xpath('dt/text()'), 'asas', x2.xpath('dd/text()')[-1])
                if not x2.xpath('dt/text()')[0] in product_body_dict.get(
                        x1.xpath('h3/text()')[0]):
                    product_body_dict.get(x1.xpath('h3/text()')[0]).update(
                        {x2.xpath('dt/text()')[0]: x2.xpath('dd/text()')[-1]})
        elif x1.xpath('dl'):
            for x2 in x1.xpath('dl'):
                print(x2.xpath('dt/text()'), 'asas', x2.xpath('dd/text()')[-1])
                if not x2.xpath('dt/text()')[0] in product_body_dict.get(
                        x1.xpath('h3/text()')[0]):
                    product_body_dict.get(x1.xpath('h3/text()')[0]).update(
                        {x2.xpath('dt/text()')[0]: x2.xpath('dd/text()')[-1]})
    # 包装清单
    if not html1.xpath('//*[@class="package-list"]/h3/text()')[
               0] in product_body_dict:
        product_body_dict.update({html1.xpath(
            '//*[@class="package-list"]/h3/text()')[0]: html1.xpath(
            '//*[@class="package-list"]/p/text()')[0]})
    print(product_body_dict)
    return product_body_dict


def product_introduction(html1):
    """
    从html中直接获取商品的介绍信息
    :param html1: 原始的html数据源
    :return: 以字典的形式返回商品介绍页面数据
    """
    html1 = etree.HTML(html1)  # 将传过来的文本转成一棵解析树
    product_intr_dict = {}  # 构建一个商品介绍字典,介绍信息以key-value存储
    # 品牌字段比较特殊,不同于其他介绍信息字段
    if not '品牌' in product_intr_dict and html1.xpath(
            '//*[@id="parameter-brand"]/li/a/text()'):  # 判断品牌字段是否在介绍信息中
        product_intr_dict.update(
            {'品牌': html1.xpath('//*[@id="parameter-brand"]/li/a/text()')[0]})
    for x3 in html1.xpath(
            '//ul[contains(@class,"parameter2")]/li'):  # 遍历商品介绍里面的介绍信息字段
        if x3.xpath('a'):  # 因为介绍信息中存在店铺字段,店铺在介绍信息中是url的存在,需要单独解析
            if x3.xpath('a') and x3.xpath('a/text()'):
                product_intr_dict.update({x3.xpath('text()')[0].split('：')[0]:
                                              x3.xpath('a/text()')[0]})
                continue
        else:  # 其余的按照正常途径解析,并且放入字典中
            if not x3.xpath('text()')[0].split('：')[0] in product_intr_dict:
                product_intr_dict.update({x3.xpath('text()')[0].split('：')[0]:
                                              x3.xpath('text()')[0].split('：')[
                                                  1]})
    print(product_intr_dict)  # 将字典输出并且返回
    return product_intr_dict


def get_comments(proxy, jd_product_id, page_count):
    """
    获取商品评论信息
    因为在调用评论接口方面很容易出错误,
    :param proxy: 代理ip信息
    :param jd_product_id: 京东码
    :param page_count: 需要抓取的评论页数
    :return:
        comment_dict:评论字典信息
        hot_comment_tag:评论标签信息
        product_comment_summary:评论描述信息
        high_praise:商品的好评度
    """
    comments_list = []
    hot_comment_tag = {}
    product_comment_summary = {}
    high_praise = {}
    # 京东对于评论接口防护非常严密,在抓取方面必须对于请求数据进行多层伪装
    max_page = 0  # 商品评论的最大页数,后面第一次解析出评论数据,将对此变量进行更新
    # 控制列表,防止出现某个商品只有一页评论,而爬虫程序仍然去请求它的第20页请求,后期页数可以考虑在提高
    page_list_1 = [i for i in range(page_count)]
    # 实际要抓取的页数列表
    page_list = [i for i in range(page_count)]
    for i in page_list_1:
        if max_page == 0:
            page_list.remove(i)
        # 接口选择列表,有四种不同的接口,每次请求不同的接口,降低某个接口的请求次数,经过多次测试,这是最符合标准的调用各个接口频率
        range_list = [1, 2, 2, 2, 3, 3, 3, 4, 4]
        range_num = random.choice(range_list)
        if max_page != 0:
            print('剩下长度', len(page_list), page_list)
            i = page_list.pop(random.randint(0, len(page_list) - 1))
            if i >= max_page:
                continue
            else:
                pass
        print('产生的是', range_num)
        if range_num == 1:
            url = 'https://club.jd.com/comment/productPageComments.action'
            payload = {'callback': 'fetchJSON_comment98',
                       'productId': '{}'.format(jd_product_id), 'score': '0',
                       'sortType': '5', 'page': str(i), 'pageSize': '10',
                       'isShadowSku': '0', 'rid': '0', 'fold': '1'}
            # 在后期爬取过程中发现调用此接口的时候会出现延迟报错状态,加上访问时间限制,并且实现错误捕捉
            try:
                response = requests.get(url=url, params=payload,
                                        headers=crawler_header.get_json_user_agent_pc(),
                                        proxies=proxy, timeout=5)
                print(response.url)
                print(response.status_code)
            except:
                time.sleep(random.uniform(1, 2))
                continue
            response_content = re.findall(r'fetchJSON_comment98[(](.*)[)];',
                                          str(response.text))
        if range_num == 2:
            url = 'https://sclub.jd.com/comment/productPageComments.action'
            payload = {'productId': '{}'.format(jd_product_id), 'score': '0',
                       'sortType': '5', 'page': str(i), 'pageSize': '10',
                       'callback': 'fetchJSON_comment98vv157'}
            try:
                response = requests.get(url=url, params=payload,
                                        headers=crawler_header.get_json_user_agent_pc(),
                                        proxies=proxy, timeout=5)
                print(response.url)
                print(response.status_code)
            except:
                time.sleep(random.uniform(1, 2))
                continue
            response_content = re.findall(
                r'fetchJSON_comment98vv157[(](.*)[)];', str(response.text))
        if range_num == 3:
            url = 'https://sclub.jd.com/productpage/p-{}-s-0-t-3-p-{}.html?callback=fetchJSON_comment98vv'.format(
                jd_product_id, i)
            try:
                response = requests.get(url=url,
                                        headers=crawler_header.get_json_user_agent_pc(),
                                        proxies=proxy, timeout=5)
                print(response.url)
                print(response.status_code)
            except:
                time.sleep(random.uniform(1, 2))
                continue
            response_content = re.findall(r'fetchJSON_comment98vv[(](.*)[)];',
                                          str(response.text))
        if range_num == 4:
            temp = random.randint(99999, 999999)
            url = 'https://sclub.jd.com/comment/productPageComments.action?callback=fetchJSON_comment98vv{}&productId={}&score=0&sortType=5&page={}&pageSize=10'.format(
                temp, jd_product_id, i)
            try:
                response = requests.get(url=url,
                                        headers=crawler_header.get_json_user_agent_pc(),
                                        proxies=proxy, timeout=5)
                print(response.url)
                print(response.status_code)
            except:
                time.sleep(random.uniform(1, 2))
                continue
            response_content = re.findall(
                r'fetchJSON_comment98vv{}[(](.*)[)];'.format(temp),
                str(response.text))
        if len(response_content) == 1:
            response_content_json = json.loads(response_content[0])
            if max_page == 0:
                if response_content_json.get('maxPage'):
                    max_page = int(response_content_json.get('maxPage'))
                    print('最大页数', max_page)
            if not high_praise:
                high_praise.update({'high_praise': response_content_json.get(
                    'productCommentSummary').get('goodRateShow')})
            if not hot_comment_tag:
                for tag in response_content_json.get(
                        'hotCommentTagStatistics'):
                    hot_comment_tag.update({tag.get('name'): tag.get('count')})
            if not product_comment_summary:
                product_tag = response_content_json.get(
                    'productCommentSummary')
                product_comment_summary.update({
                    'afterCountStr': product_tag.get('afterCountStr',
                                                     '0')})  # 追评
                product_comment_summary.update({
                    'commentCountStr': product_tag.get('commentCountStr',
                                                       '0')})  # 全部评价
                product_comment_summary.update({
                    'generalCountStr': product_tag.get('generalCountStr',
                                                       '0')})  # 中评价
                product_comment_summary.update({
                    'goodCountStr': product_tag.get('goodCountStr',
                                                    '0')})  # 好评
                product_comment_summary.update({
                    'poorCountStr': product_tag.get('poorCountStr',
                                                    '0')})  # 差评
                product_comment_summary.update({
                    'videoCountStr': product_tag.get('videoCountStr',
                                                     '0')})  # 晒视频
                product_comment_summary.update({
                    'imageListCount': response_content_json.get(
                        'imageListCount', '0')})
            comments = response_content_json.get('comments')
            if comments:
                for i, comment in enumerate(comments):
                    print(i, comment['content'])
                    # 定义一个匿名方法,处理评论信息中商品名称和规格为空的状态
                    func = lambda x: x if x else 'NULL'
                    # 定义一个匿名方法,处理评论中是否有图片信息或者视频信息
                    func_1 = lambda x: 1 if x else 0
                    comments_list.append((comment.get('id'),
                                          comment.get('content'),
                                          comment.get('score'),
                                          comment.get('creationTime'), func(
                        comment.get('productColor', '') + comment.get(
                            'productSize', '')),
                                          comment.get('usefulVoteCount'),
                                          comment.get('replyCount'),
                                          func_1(comment.get('images', '')),
                                          func_1(comment.get('videos', ''))))
                time.sleep(random.uniform(1, 2))
            else:
                break
        else:
            print('没有检索到')
            time.sleep(random.uniform(1, 2))
    comment_dict = {'comments': comments_list}
    hot_comment_tag = {'hot_commentTag': hot_comment_tag}
    product_comment_summary = {
        'product_CommentSummary': product_comment_summary}
    return comment_dict, hot_comment_tag, product_comment_summary, high_praise


def see_and_see_list(proxy, jd_product_id):
    """
    发出json请求,获取此商品推荐的看了又看信息
    :param proxy: 代理ip
    :param jd_product_id: 京东码
    :return: 以字典的形式返回看了又看的商品信息{'see_and_see':{}}
    """
    see_dict = {'see_and_see': {}}
    url = ' https://diviner.jd.com/diviner'
    global see_count
    global see_timec
    if see_count % 3 == 0:  # 如果计数变量可以被3整除,则对于时间戳进行更新
        see_timec = str(int(time.time())) + '{}'.format(
            random.randint(100, 999))
        payload = {'lid': '15', 'lim': '15', 'ec': 'utf-8',
                   'uuid': '2037093595', 'ck': 'pin,ipLocation,atw,aview',
                   'pin': '', 'p': '902029', 'callback': 'jQuery6431766',
                   '_': see_timec, 'sku': '{}'.format(
                str(jd_product_id))}  # 因为这个时间时间戳已经改变,就要对时间戳变量进行改变
    else:
        payload = {'lid': '15', 'lim': '15', 'ec': 'utf-8',
                   'uuid': '2037093595', 'ck': 'pin,ipLocation,atw,aview',
                   'pin': '', 'p': '902029', 'callback': 'jQuery6431766',
                   '_': see_timec, 'sku': '{}'.format(str(jd_product_id))}
    see_count += 1  # 每进行一个商品的处理,计数变量加一
    try:
        see_response = requests.get(url=url, params=payload,
                                    headers=crawler_header.get_json_user_agent_pc(),
                                    proxies=proxy, timeout=5)
        print(see_response.status_code)
    except:
        print('调用接口出现错误')
        return None
    see_response_content = re.findall(r'jQuery6431766[(](.*)[)]',
                                      str(see_response.text))
    if len(see_response_content) > 0:
        see_response_content_json = json.loads(see_response_content[0])
        see_product_list = see_response_content_json.get('data')
        for see_product in see_product_list:
            see_dict.get('see_and_see').update(
                {see_product.get('t'): see_product.get('sku')})
    else:
        print('没有检索到')
        return None
    return see_dict


# 为你推荐连接,用于程序失效时
# Request URL: https://diviner.jd.com/diviner?lid=6&lim=12&ec=utf-8&uuid=56171293&pin=
# jd_4d67a4592377b&p=102000&sku=100013157242&ck=pin&c1=12218&c2=21455&c3=21456&callback=jQuery9900517&_=1621566252510
def recommend_it_to_you(proxy, jd_product_id):
    """
    发出json请求,获取此商品推荐的为你推荐商品信息
    :param proxy: 代理ip信息
    :param jd_product_id: 京东码
    :return: 以字典的形式返回为你推荐信息
    """
    recommend_for_you_product_dict = {'recommend_it_to_you': {}}
    url = ' https://diviner.jd.com/diviner'
    global recommend_count
    global recommend_timec
    if recommend_count % 3 == 0:
        recommend_timec = str(int(time.time())) + '{}'.format(
            random.randint(100, 999))
        payload = {'lid': '6', 'lim': '12', 'ec': 'utf-8', 'uuid': '56171293',
                   'pin': '', 'p': '102000', 'callback': 'jQuery9900517',
                   '_': recommend_timec,
                   'sku': '{}'.format(str(jd_product_id))}
        print(payload)
    else:
        payload = {'lid': '6', 'lim': '12', 'ec': 'utf-8', 'uuid': '56171293',
                   'pin': '', 'p': '102000', 'callback': 'jQuery9900517',
                   '_': recommend_timec,
                   'sku': '{}'.format(str(jd_product_id))}
    recommend_count += 1
    try:
        recommend_for_you_response = requests.get(url=url, params=payload,
                                                  headers=crawler_header.get_user_agent_pc(),
                                                  proxies=proxy, timeout=5)
        print(recommend_for_you_response.status_code)
    except:
        print('报错了')
        return None
    recommend_for_you_response_content = re.findall(r'jQuery9900517[(](.*)[)]',
                                                    str(
                                                        recommend_for_you_response.text))
    if len(recommend_for_you_response_content) > 0:
        recommend_for_you_response_content_json = json.loads(
            recommend_for_you_response_content[0])
        recommend_for_you_product_list = recommend_for_you_response_content_json.get(
            'data')
        for recommend_for_you_product in recommend_for_you_product_list[0:31]:
            if recommend_for_you_product:
                recommend_for_you_product_dict.get(
                    'recommend_it_to_you').update({
                    recommend_for_you_product.get(
                        't'): recommend_for_you_product.get('sku')})
    else:
        print('没有检索到')
        return None
    return recommend_for_you_product_dict


def get_question_answer_list(proxy, jd_product_id, page_count):
    """
    发出json请求,获取商品的问答信息,并且实现数据格式化
    :param proxy: 代理ip信息
    :param jd_product_id: 京东码
    :param page_count: 需要抓取的页数
    :return:  商品的问答字典
    """
    question_dict = {}
    for i in range(1, page_count):
        url = 'https://question.jd.com/question/getQuestionAnswerList.action'
        payload = {'callback': 'jQuery1911861', 'page': '{}'.format(i),
                   'productId': '{}'.format(str(jd_product_id))}
        # 在请求接口时会出现访问接口超时的现象,需要做错误异常捕捉
        try:
            question_answer_response = requests.get(url=url, params=payload,
                                                    headers=crawler_header.get_user_agent_pc(),
                                                    proxies=proxy, timeout=5)
            questiont_response_content = re.findall(r'jQuery1911861[(](.*)[)]',
                                                    str(
                                                        question_answer_response.text))
        except:
            time.sleep(random.uniform(0.5, 1))
            continue
        if len(questiont_response_content) > 0:
            question_response_content_json = json.loads(
                questiont_response_content[0])
            if 'questionList' in question_response_content_json:
                question_list = question_response_content_json.get(
                    'questionList')
                for question in question_list:
                    question_id = question.get('id')
                    question_content = question.get('content')
                    answer_count = question.get('answerCount')
                    create_time = question.get('created')
                    question_dict.update({question_id: [question_content,
                                                        answer_count,
                                                        create_time]})
                    if question.get('answerList'):
                        answer_id = question.get('id')
                        answer_content = question.get('answerList')[0].get(
                            'content')
                        answer_create_time = question.get('answerList')[0].get(
                            'created')
                        question_dict.get(answer_id).extend(
                            [answer_content, answer_create_time])
                    else:
                        pass
            else:
                print('商品在此页没有问答')
                break
        else:
            print('没有检索到')
        time.sleep(random.uniform(0.5, 1))
    return question_dict


def jd_xpath_main(response, proxy, jd_product_id, comment_page, question_page):
    """
    将所需要的信息从页面中提取出来,组成数据包
    :param response: 请求返回的请求体
    :param proxy: 代理ip的信息
    :param jd_product_id: 京东码
    :param comment_page: 是需要爬取的评论页数
    :param question_page: 所需要爬取的问答页数
    :return: 组装好的数据包
    """
    html1 = etree.HTML(response.text)  # 将返回的html页面解析成一个树,便于后期的信息提取
    product_intr = html1.xpath(
        '//*[@id="detail"]/div[2]/div[1]/div[1]')  # 商品介绍模块
    product_body = html1.xpath('//*[@id="detail"]/div[2]/div[2]')  # 商品主体模块
    print(product_intr, product_body)
    all_dict = {}  # 创建一个大字典,后期所有的介绍信息存入大字典中,便于解析数据和存储
    product_intr_tree1 = html.tostring(product_intr[0])  # 对于上面产生的商品介绍继续解析
    product_intr_tree2 = HTMLParser().unescape(
        product_intr_tree1.decode('utf-8'))
    product_intr_dict = product_introduction(product_intr_tree2)
    product_body_tree1 = html.tostring(product_body[0])  # 对于上面产生的商品主体继续解析
    product_body_tree2 = HTMLParser().unescape(
        product_body_tree1.decode('utf-8'))
    product_body_dict = product_body_func(product_body_tree2)
    # 将解析出来的商品介绍字典信息保存至all_dict字典
    all_dict.update({'shaopinjieshao': product_intr_dict})
    # 将解析出来的商品规格与包装字典信息保存至all_dict字典
    all_dict.update({'guige_yubaozhuang': product_body_dict})
    # 店铺名称
    shop_name, is_self = product_shop_name_is_self(html1)
    # 将店铺名称以字典形式存入all_dict
    all_dict.update({'shop_name': shop_name})
    all_dict.update({'is_self': is_self})
    # 店铺星级数量
    shop_star_count = shop_star_count_func(html1)
    # 将店铺的星级数量以字典形式存入all_dict
    all_dict.update({'good_star_num': shop_star_count})
    type_list = type_name(html1)
    if not type_list:  # 如果商品类别解析失败,证明页面存在问题,终止程序继续解析
        return False
    all_dict.update({'bread_name': type_list})
    # 将返回的商品类别信息以字典的形式存入all_dict中
    try:
        range_list = [i for i in range(16)]
        range_num = random.choice(range_list)
        if range_num % 5 == 0:
            if 'track-tit' in response.text:
                print('存在看了有看')
                see_dict = see_and_see_list(proxy, jd_product_id)
                if see_dict:
                    pass
                else:
                    # 将看了又看字典存入all_dict中
                    all_dict.update(see_dict)
            if '为你推荐' in response.text:
                print('存在为你推荐')
                recommend_for_you_product_dict = recommend_it_to_you(proxy,
                                                                     jd_product_id)
                if recommend_for_you_product_dict:
                    pass
                else:
                    # 将为你推荐字典存入all_dict中
                    all_dict.update(recommend_for_you_product_dict)
    except:
        pass
    product_name = product_name_func(html1)
    # 将商品名称字典存入all_dict中
    all_dict.update({"good_name": product_name})
    # 商品价格,如果接口调用失败的话,返回两个0,0
    present_price, former_price = product_price(proxy, jd_product_id)
    # 将商品的现价字典存储至all_dict
    all_dict.update({"present_price": present_price})
    # 将商品的原价字典存储至all_dict
    all_dict.update({'former_price': former_price})
    # 商品标签
    product_tag = product_tag_func(html1)
    # all_dict存储商品tag
    all_dict.update({"good_tag": product_tag})
    # 商品的可选种类
    type_dict = data_product_type(html1)
    # all_dict存储商品可选商品信息
    all_dict.update({'data_goods_type': type_dict})
    # 返回评论字典,评论标签字典
    comment_dict, hot_comment_tag, product_comment_summary, high_praise = get_comments(
        proxy=proxy, jd_product_id=jd_product_id, page_count=comment_page)
    # 评论信息字典,评论标签字典,评论描述信息字典,商品的好评度字典
    all_dict.update(comment_dict)  # 评论字典
    all_dict.update(hot_comment_tag)  # 商品标签
    all_dict.update(product_comment_summary)  # 评论标签字典
    all_dict.update(high_praise)  # 好评度
    question_dict = get_question_answer_list(proxy=proxy,
                                             jd_product_id=jd_product_id,
                                             page_count=question_page)
    # all_dict存储问答字典
    all_dict.update({'question_dict': question_dict})
    return all_dict
