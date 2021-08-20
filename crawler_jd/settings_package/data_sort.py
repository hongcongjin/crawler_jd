# -*- coding: UTF-8 -*-
"""
Author: Sunck
note: 对于线程调度下的数据包进行标准化解析,并且实现入库
Created time: 2021/06/28
"""
from pymysql.converters import escape_string
from collections import deque
from settings_package.db_function import python_sql_mysql


def number_tr(str):
    """
    将评论描述信息中的10万+ ,转成int型
    100万+(str),需要函数来将它处理成,1000000(int)
    :param str: 需要转化的字符串
    :return: 字符串相对应的整型数字
    """
    if type(str) == int:  # 如果传过来的就是int数据,直接将其返回,例如:148
        return str
    elif '+' in str:  # 如果传过来数据中有+,1000+
        count_a = str.split('+')[0]
        if '万' in count_a:  # 如果传过来数据中有+,1万+
            if '.' in count_a:
                count_b = int(float(count_a.split('万')[0]) * 10000)
                return count_b
            count_b = int(count_a.split('万')[0]) * 10000
            print(count_b)
            return count_b
        else:
            return int(count_a)
    else:
        return int(str)


def detail_jd_data_product_info(jd_product_id, crawler_dict, db_name):
    """
    处理传过来的爬虫数据字典,将商品详细信息进行数据组装,组装sql语句
    :param jd_product_id: 京东码
    :param crawler_dict: 解析好的json数据
    :return: 不返回
    """
    jd_product_name = crawler_dict.get('good_name')  # crawler_dict获得商品名称
    if jd_product_name:
        pass
    else:
        jd_product_name = crawler_dict.get('shaopinjieshao').get('商品名称',
                                                                 '无法检索商品名称')  # 部分商品名称在商品的介绍信息中也存在
    former_price = crawler_dict.get('former_price', 0)  # crawler_dict获得商品现价
    present_price = crawler_dict.get('present_price', 0)  # crawler_dict获得商品原价
    if crawler_dict.get('shop_name'):
        shop_name = crawler_dict.get('shop_name')[0]  # crawler_dict获得商品店铺名称
    else:
        shop_name = 'NULL'  # 如果无法获取则为空
    if crawler_dict.get('good_star_num'):  # crawler_dict获得商品商家星级标准
        shop_star_count = crawler_dict.get('good_star_num')[0]
    else:
        shop_star_count = 'NULL'  # 如果无法获取星级数,则为空
    type_list = crawler_dict.get(
        'bread_name')  # crawler_dict获得商品所属类别,部分商品的商品所属类别有可能不是标准的六个,需要判断
    if len(type_list) == 6:  # 当商品的所属类别为6个时,数据标准无需处理
        pass
    elif len(type_list) == 5:  # 当商品的所属类别为5个时,数据需要添加2个空值
        type_list.append('NULL')
    elif len(type_list) == 4:
        type_list.extend(['NULL', 'NULL'])  # 当商品的所属类别为4个时,数据需要添加2个空值
    elif len(type_list) == 3:
        type_list.extend(['NULL', 'NULL', 'NULL'])
    elif len(type_list) == 2:
        type_list.extend(['NULL', 'NULL', 'NULL', 'NULL'])
    coupon = 'NULL'  # 商品优惠劵信息,前面无法处理,等待优化
    promotion_sale = 'NULL'  # 商品促销信息,前面无法处理,等待优化
    if crawler_dict.get('is_self') == 'False':  # crawler_dict获得商品店铺是否为自营
        is_self_support = 0
    if crawler_dict.get('is_self') == 'True':
        is_self_support = 1
    # good_tag会出现两种情况     一种返回Null,一种返回真实的tag值
    product_tag = crawler_dict.get('good_tag')  # crawler_dict获得商品的标签信息
    tu1 = (
        jd_product_id, jd_product_name, present_price, former_price, shop_name,
        shop_star_count, type_list[0], type_list[1], type_list[2],
        type_list[3], type_list[4], type_list[5], coupon, promotion_sale,
        is_self_support, product_tag)
    select_sql = "select jd_product_id from jd_data_product_info where jd_product_id=%s" % (
        tu1[0])
    if python_sql_mysql(db_name=db_name, sql=select_sql, is_return=True):
        pass
    else:
        insert_sql = "insert into jd_data_product_info (jd_product_id," \
                     "jd_product_name,present_price,former_price," \
                     "shop_name,shop_star_count,category_1,category_2," \
                     "category_3,category_4,category_5,category_6," \
                     "coupon,promotion_sale,is_self_support,product_tag" \
                     ") VALUES (%s,\'%s\',%s,%s,\'%s\',%s,\'%s\',\'%s\',\'%s\'," \
                     "\'%s\',\'%s\',\'%s\',\'%s\',\'%s\',%s,\'%s\')" % (
                         tu1[0], escape_string(tu1[1]), tu1[2], tu1[3],
                         escape_string(tu1[4]), tu1[5], escape_string(tu1[6]),
                         escape_string(tu1[7]), escape_string(tu1[8]),
                         escape_string(tu1[9]), escape_string(tu1[10]),
                         escape_string(tu1[11]), tu1[12], tu1[13], tu1[14],
                         escape_string(tu1[15]))
        print(insert_sql)
        python_sql_mysql(db_name=db_name,
                         sql=insert_sql.replace('\'NULL\'', 'NULL'))


def detail_jd_data_reco_info(jd_product_id, crawler_dict, db_name):
    """
    处理传过来的爬虫数据字典,将商品推荐信息进行数据组装,组装sql语句
    :param jd_product_id: 京东码
    :param crawler_dict: 解析完毕的json数据
    :return: 不返回
    """
    for index, reco_type in enumerate(['see_and_see', 'recommend_it_to_you']):
        if crawler_dict.get(reco_type):
            for key in crawler_dict.get(reco_type).keys():
                reco_l = deque()  # 解析的每个商品的每个字段信息需要有序存储,使用deque
                reco_l.append(jd_product_id)
                reco_l.append(key)
                reco_l.append(int(crawler_dict.get(reco_type).get(key)))
                reco_l.append(index)
                # 对产生的reco_l数据进行入库操作
                select_sql = "select jd_product_id from jd_data_reco_info " \
                             "WHERE jd_product_id=%s and recommend_id=%s " \
                             "and recommend_type=%s" % (
                                 reco_l[0], reco_l[2], reco_l[3])
                if python_sql_mysql(db_name=db_name, sql=select_sql,
                                    is_return=True):
                    pass
                else:
                    insert_sql = "insert into jd_data_reco_info " \
                                 "(jd_product_id,recommend_name," \
                                 "recommend_id,recommend_type) " \
                                 "values (%s,\'%s\',%s,%s)" % (
                                     reco_l[0], escape_string(reco_l[1]),
                                     reco_l[2], reco_l[3])
                    python_sql_mysql(db_name=db_name, sql=insert_sql)
        else:
            pass


def detail_jd_data_product_type(jd_product_id, crawler_dict, db_name):
    """
    处理传过来的爬虫数据字典,将商品推荐信息进行数据组装,组装sql语句
    :param jd_product_id: 京东码
    :param crawler_dict: 解析好的json数据
    :return: 不返回
    """
    keys = list(crawler_dict.get('data_goods_type').keys())
    # 使用列表生成式来完成复杂的数据组成工作
    if keys:
        tu1_lists = [[(int(jd_product_id), i, j) for j in
                      crawler_dict.get('data_goods_type').get(i)] for i in
                     keys]
        for tu1_list in tu1_lists:
            for tu1 in tu1_list:
                select_sql = "select jd_product_id from jd_data_product_type " \
                             "where jd_product_id=%s and " \
                             "type_value=\'%s\'" % (
                                 tu1[0], escape_string(tu1[2]))
                if python_sql_mysql(db_name=db_name, sql=select_sql,
                                    is_return=True):
                    pass
                else:
                    insert_sql = "insert into jd_data_product_type " \
                                 "(jd_product_id,type_key,type_value) " \
                                 "VALUES (%s,\'%s\',\'%s\')" % (
                                     tu1[0], escape_string(tu1[1]),
                                     escape_string(tu1[2]))
                    python_sql_mysql(db_name=db_name, sql=insert_sql)
    else:
        pass


def detail_jd_data_intr_detail(jd_product_id, crawler_dict, db_name):
    """
    处理传递过来的爬虫数据字典,将商品的介绍信息进行数据组装,组装sql语句
    :param jd_product_id: 京东码
    :param crawler_dict: 解析好的json数据
    :return: 不返回
    """
    detail_key = crawler_dict.get('shaopinjieshao')
    for key in detail_key.keys():
        intr_l = deque()  # 介绍信息里面的每条信息中的内容必须要求顺序,采用deque()来存储
        intr_l.append(jd_product_id)
        intr_l.append(key)
        intr_l.append(detail_key.get(key))
        select_sql = "select jd_product_id from jd_data_intr_detail where " \
                     "jd_product_id=%s and detail_value=\'%s\'" % (
                         intr_l[0], escape_string(intr_l[2]))
        if python_sql_mysql(db_name=db_name, sql=select_sql, is_return=True):
            pass
        else:
            insert_sql = "insert into jd_data_intr_detail " \
                         "(jd_product_id,detail_key,detail_value) " \
                         "VALUES (%s,\'%s\',\'%s\')" % (
                             intr_l[0], escape_string(intr_l[1]),
                             escape_string(intr_l[2]))
            python_sql_mysql(db_name=db_name, sql=insert_sql)


def detail_jd_data_spec_pack_detail(jd_product_id, crawler_dict, db_name):
    """
    处理传递过来的爬虫数据字典,将商品的规格与包装信息进行数据组装,组装sql语句
    :param jd_product_id: 京东码
    :param crawler_dict: 解析好的json数据
    :return: 不返回
    """
    temp = crawler_dict.get('guige_yubaozhuang')  # 中间变量
    for i in temp.keys():
        # 存在两个情况,一种是信息会存在两个键,一个值、一种是信息会存在一个键一个值。分开处理
        if type(temp.get(i)) == dict:
            for j in temp.get(i).keys():
                pack_l = deque()  # 规格与包装信息里面的每条信息的内容必须要有序存储,采用deque()
                pack_l.append(jd_product_id)
                pack_l.append(i)
                pack_l.append(j)
                pack_l.append(temp.get(i).get(j))
                select_sql = "select jd_product_id from jd_data_spec_pack_detail " \
                             "where jd_product_id=%s and detail_key_2=\'%s\'" % (
                                 pack_l[0], escape_string(pack_l[2]))
                if python_sql_mysql(db_name=db_name, sql=select_sql,
                                    is_return=True):
                    pass
                else:
                    insert_sql = "insert into jd_data_spec_pack_detail " \
                                 "(jd_product_id,detail_key_1,detail_key_2," \
                                 "detail_value) VALUES (%s,\'%s\',\'%s\',\'%s\')" % (
                                     pack_l[0], escape_string(pack_l[1]),
                                     escape_string(pack_l[2]),
                                     escape_string(pack_l[3]))
                    python_sql_mysql(db_name=db_name, sql=insert_sql)
        else:
            pack_l = deque()
            pack_l.append(jd_product_id)
            pack_l.append(i)
            pack_l.append('NULL')
            pack_l.append(temp.get(i))
            print(pack_l)
            if pack_l[2] == 'NULL':
                select_sql = "select jd_product_id from jd_data_spec_pack_detail " \
                             "where jd_product_id=%s and detail_key_2 is null" % (
                                 pack_l[0])
            else:
                select_sql = "select jd_product_id from jd_data_spec_pack_detail " \
                             "where jd_product_id=%s and detail_key_2=\'%s\'" % (
                                 pack_l[0], escape_string(pack_l[2]))
            if python_sql_mysql(db_name=db_name, sql=select_sql,
                                is_return=True):
                pass
            else:
                insert_sql = "insert into jd_data_spec_pack_detail " \
                             "(jd_product_id,detail_key_1,detail_key_2," \
                             "detail_value) VALUES (%s,\'%s\',\'%s\',\'%s\')" % (
                                 pack_l[0], escape_string(pack_l[1]),
                                 escape_string(pack_l[2]),
                                 escape_string(pack_l[3]))
                python_sql_mysql(db_name=db_name,
                                 sql=insert_sql.replace('\'NULL\'', 'NULL'))


def detail_jd_data_comment_summary(jd_product_id, crawler_dict, db_name):
    """
    处理传递过来的爬虫数据字典,将商品评论介绍信息进行数据组装,组装sql语句
    :param jd_product_id: 京东码
    :param crawler_dict: 解析好的json数据
    :return: 不返回
    """
    temp = crawler_dict.get('product_CommentSummary')  # 中间变量
    if temp:
        all_count = number_tr(temp.get('commentCountStr'))  # 总评论数
        image_count = number_tr(temp.get('imageListCount'))  # 带图评论数
        video_count = number_tr(temp.get('videoCountStr'))  # 带食品评论数
        great_count = number_tr(temp.get('generalCountStr'))  # 好评数
        poor_count = number_tr(temp.get('poorCountStr'))  # 差评数
        general_count = number_tr(temp.get('generalCountStr'))  # 中评数
        after_count = number_tr(temp.get('afterCountStr'))  # 追评数
        high_opinion_rate = int(crawler_dict.get('high_praise'))  # 好评度
        tu1 = (jd_product_id, all_count, image_count, video_count, great_count,
               poor_count, general_count, after_count, high_opinion_rate)
        select_sql = "select jd_product_id from jd_data_comment_summary " \
                     "where jd_product_id=%s" % (tu1[0])
        if python_sql_mysql(db_name=db_name, sql=select_sql, is_return=True):
            pass
        else:
            insert_sql = "insert into jd_data_comment_summary " \
                         "(jd_product_id,all_count,image_count," \
                         "video_count,great_count,poor_count,general_count," \
                         "after_count,high_opinion_rate) VALUES " \
                         "(%s,%s,%s,%s,%s,%s,%s,%s,%s)" % (
                             tu1[0], tu1[1], tu1[2], tu1[3], tu1[4], tu1[5],
                             tu1[6], tu1[7], tu1[8])
            python_sql_mysql(db_name=db_name, sql=insert_sql)


def detail_jd_data_comment_label(jd_product_id, crawler_dict, db_name):
    """
    处理传递过来的爬虫数据字典,将商品评论标签信息进行数据组装,组装sql语句
    :param jd_product_id: 京东码
    :param crawler_dict: 解析好的json数据
    :return: 不返回
    """
    temp = crawler_dict.get('hot_commentTag')  # 中间变量
    for key in temp:
        comment_label_l = deque()  # 每个标签内部的信息必须有序存储,后期需要插入mysql中
        comment_label_l.append(jd_product_id)
        comment_label_l.append(key)
        comment_label_l.append(temp.get(key))
        select_sql = "select jd_product_id from jd_data_comment_label " \
                     "where jd_product_id=%s " \
                     "and label_name=\'%s\'" % (
                         comment_label_l[0], escape_string(comment_label_l[1]))
        if python_sql_mysql(db_name=db_name, sql=select_sql, is_return=True):
            pass
        else:
            insert_sql = "insert into jd_data_comment_label (jd_product_id," \
                         "label_name,label_count) VALUES (%s,\'%s\',%s)" % (
                             comment_label_l[0],
                             escape_string(comment_label_l[1]),
                             comment_label_l[2])
            python_sql_mysql(db_name=db_name, sql=insert_sql)


def detail_jd_data_comment_detail(jd_product_id, crawler_dict, db_name):
    """
    处理传递过来的爬虫数据字典,将商品评论内容信息进行数据组装,组装sql语句
    :param jd_product_id: 京东码
    :param crawler_dict: 解析好的json数据
    :return: 不返回
    """
    comments = crawler_dict.get('comments')  # 中间变量
    for comment in comments:
        comment_detail_l = deque()  # 一条评论信息内部的所有信息必须有序存储
        comment_detail_l.append(jd_product_id)
        comment_detail_l.extend(comment)
        select_sql = "select jd_product_id from jd_data_comment_detail " \
                     "where jd_product_id=%s and comment_id=%s" % (
                         comment_detail_l[0], comment_detail_l[1])
        if python_sql_mysql(db_name=db_name, sql=select_sql, is_return=True):
            pass
        else:
            insert_sql = "insert into jd_data_comment_detail (jd_product_id," \
                         "comment_id,content,score_count,create_time," \
                         "product_name,like_count,reply_count,is_image," \
                         "is_video) VALUES (%s,%s,\'%s\',%s,\'%s\',\'%s\',%s,%s,%s,%s)" % (
                             comment_detail_l[0], comment_detail_l[1],
                             escape_string(comment_detail_l[2]),
                             comment_detail_l[3],
                             escape_string(comment_detail_l[4]),
                             escape_string(comment_detail_l[5]),
                             comment_detail_l[6], comment_detail_l[7],
                             comment_detail_l[8], comment_detail_l[9])
            python_sql_mysql(db_name=db_name,
                             sql=insert_sql.replace('\'NULL\'', 'NULL'))


def detail_jd_data_question(jd_product_id, crawler_dict, db_name):
    """
    处理传递过来的爬虫数据字典,将商品问答内容信息进行数据组装,组装sql语句
    :param jd_product_id: 京东码
    :param crawler_dict: 解析好的json数据
    :return: 不返回
    """
    temp = crawler_dict.get('question_dict')  # 中间变量,存储所有问题的
    for i in temp.keys():  # 遍历所有的评论id
        question_l = deque()  # 每条评论内部信息是有序存储
        question_l.append(jd_product_id)  # 添加商品码
        question_l.append(i)  # 添加问题id
        for j in temp.get(i):
            question_l.append(j)  # 添加问题内容,点赞数,问题时间,回复内容,回复时间
        if len(temp.get(i)) == 3:  # 判断   某些问题不存在回复内容,回复时间  添加两个空值
            question_l.append('NULL')
            question_l.append('NULL')
        select_sql = "select jd_product_id,question_id from jd_data_question " \
                     "WHERE jd_product_id=%s and question_id=%s" % (
                         question_l[0], question_l[1])
        if python_sql_mysql(db_name=db_name, sql=select_sql, is_return=True):
            pass
        else:
            insert_sql = "insert into jd_data_question (jd_product_id," \
                         "question_id,question_content,answer_count," \
                         "question_create_time,answer_content," \
                         "answer_create_time) VALUES " \
                         "(%s,%s,\'%s\',%s,\'%s\',\'%s\',\'%s\')" % (
                             question_l[0], question_l[1],
                             escape_string(question_l[2]), question_l[3],
                             escape_string(question_l[4]),
                             escape_string(question_l[5]),
                             escape_string(question_l[6]))
            python_sql_mysql(db_name=db_name,
                             sql=insert_sql.replace('\'NULL\'', 'NULL'))


def data_sort_main(jd_product_id, crawler_dict, db_name):
    """
    对于爬虫数据进行格式化,入库操作
    :param jd_product_id: 商品的京东码
    :param crawler_dict: 解析出来的爬虫数据字典
    :param db_name: 数据入库的库名
    :return: 不返回
    """
    # 处理商品的推荐信息
    detail_jd_data_reco_info(jd_product_id, crawler_dict, db_name=db_name)
    # 处理商品的可选规格信息
    detail_jd_data_product_type(jd_product_id, crawler_dict, db_name=db_name)
    # 处理商品的介绍信息
    detail_jd_data_intr_detail(jd_product_id, crawler_dict, db_name=db_name)
    # 处理商品的规格与包装信息
    detail_jd_data_spec_pack_detail(jd_product_id, crawler_dict,
                                    db_name=db_name)
    # 处理商品的评论介绍信息
    detail_jd_data_comment_summary(jd_product_id, crawler_dict,
                                   db_name=db_name)
    # 处理商品的评论标签信息
    detail_jd_data_comment_label(jd_product_id, crawler_dict, db_name=db_name)
    # 处理商品的评论内容信息
    detail_jd_data_comment_detail(jd_product_id, crawler_dict, db_name=db_name)
    # 处理商品的问答方面信息
    detail_jd_data_question(jd_product_id, crawler_dict, db_name=db_name)
    # 处理商品的基本信息
    detail_jd_data_product_info(jd_product_id, crawler_dict, db_name=db_name)
