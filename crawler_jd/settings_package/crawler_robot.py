# -*- coding: UTF-8 -*-
# ！/root/.virtualenvs/test/bin/python3.7
"""
Author: Sunck
note: 爬虫报警机器人
Created time: 2021/06/28
"""
import json
import requests


def send_msg(jd_product_id):
    """
    爬虫警告机器人,用于爬虫程序异常时发出相应的警告信息
    :return: 不返回
    """
    data = json.dumps({"msgtype": "text", "text": {
        "content": "crawler主线程出现异常{}".format(str(jd_product_id)),
        "mentioned_list": ["hongcongjin"], }})
    url = 'https://qyapi.weixin.qq.com/cgi-bin/webhook/send?key=d40e7d65-2ffc-4a69-b263-8413e73fcd1f'
    requests.post(url, data, auth=('Content-Type', 'application/json'))
