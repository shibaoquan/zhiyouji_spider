import time

import requests
import random

from pymongo import MongoClient

import user_agent
from lxml import etree
import json

import threading
from queue import Queue


class ZhiYouJi(object):
    """
    多线程 职友集爬虫
    """

    def __init__(self):
        self.base_url = "https://www.jobui.com/jobs?jobKw=python&cityKw=%E5%8C%97%E4%BA%AC&"
        # self.header = {"User-Agent": random.choice(user_agent.agents)}
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US) AppleWebKit/532.5 (KHTML, like Gecko) Chrome/4.0.249.0 Safari/532.5"}

        self.begin_page = 1
        self.end_page = 100

        # 1. url队列
        self.url_queue = Queue()
        # 2. 列表url对列
        self.detail_url_queue = Queue()
        # 2. 响应对列
        self.reaponse_queue = Queue()
        # 3. 数据队列
        self.data_queue = Queue()

        # 记录个数
        self.count = 0

    def get_url_list(self):

        for i in range(self.begin_page, self.end_page):
            # url入队列
            self.url_queue.put(self.base_url.format(i))

    def send_request(self):

        while True:
            # 取出队列中的url
            url = self.url_queue.get()
            response = requests.get(url, headers=self.headers)
            # print(response.url)

            if response.status_code == 200:
                # 入列表url对列
                data = response.content.decode()
                html = etree.HTML(data)
                detail_url_list = html.xpath('//div[@class="cfix"]//div[@class="cfix"]/a/@href')
                for detail_url in detail_url_list:
                    detail_url = "https://www.jobui.com" + detail_url
                    self.detail_url_queue.put(detail_url)

            else:
                self.url_queue.put(url)

            # 计数器减一
            self.url_queue.task_done()

    def detail_request(self):

        while True:
            # 取出列表url对列中的数据
            detail_url = self.detail_url_queue.get()
            # print(detail_url)

            # 请求
            detail_response = requests.get(detail_url, headers=self.headers).content.decode()

            self.reaponse_queue.put(detail_response)

    def analysic(self):

        list = []
        while True:
            # 取出响应对列中的数据
            detail_data = self.reaponse_queue.get()
            # 解析
            html = etree.HTML(detail_data)

            work_dict = {}
            work_dict["work_possion"] = html.xpath('//*[@id="wrapcontainer"]/div[2]/div[1]/div[1]/div[1]/h1/text()')[
                0].strip() if len(
                html.xpath('//*[@id="wrapcontainer"]/div[2]/div[1]/div[1]/div[1]/h1/text()')) > 0 else ''
            work_dict["work_sllary"] = \
                html.xpath('//*[@id="wrapcontainer"]/div[2]/div[1]/div[1]/div[1]/ul/li[4]/span[2]/text()')[
                    0].strip() if len(
                    html.xpath(
                        '//*[@id="wrapcontainer"]/div[2]/div[1]/div[1]/div[1]/ul/li[4]/span[2]/text()')) > 0 else ''
            work_dict["work_city"] = html.xpath('//*[@id="wrapcontainer"]/div[2]/div[1]/div[1]/div[1]/ul/li[1]/text()')[
                0].strip() if len(
                html.xpath('//*[@id="wrapcontainer"]/div[2]/div[1]/div[1]/div[1]/ul/li[1]/text()')) > 0 else ''
            work_dict["work_jingyan"] = html.xpath('//*[@id="jobAge"]/text()')[0].strip() if len(
                html.xpath('//*[@id="jobAge"]/text()')) > 0 else ''
            work_dict["work_xueli"] = \
                html.xpath('//*[@id="wrapcontainer"]/div[2]/div[1]/div[1]/div[1]/ul/li[5]/text()')[0].strip() if len(
                    html.xpath('//*[@id="wrapcontainer"]/div[2]/div[1]/div[1]/div[1]/ul/li[5]/text()')) > 0 else ''
            work_dict["work_address"] = \
                html.xpath('//*[@id="wrapcontainer"]/div[2]/div[1]/div[1]/div[1]/ul/li[6]/text()')[0].strip() if len(
                    html.xpath('//*[@id="wrapcontainer"]/div[2]/div[1]/div[1]/div[1]/ul/li[6]/text()')) > 0 else ''
            work_dict["work_content"] = \
                html.xpath('//*[@id="wrapcontainer"]/div[2]/div[1]/div[1]/div[1]/div[3]/div/text()')[0].strip() if len(
                    html.xpath('//*[@id="wrapcontainer"]/div[2]/div[1]/div[1]/div[1]/div[3]/div/text()')) > 0 else ''

            json_str = json.dumps(work_dict, ensure_ascii=False) + '\n'

            #  入数据 队列
            self.data_queue.put(json_str)
            # 计数器减一
            self.reaponse_queue.task_done()

    def save_data(self):

        """数据存入mongodb"""

        con = MongoClient(host='127.0.0.1', port=27017)  # 实例化MongoClient
        self.collection = con.zhaopin.zhiyouji_zhaopin  # 创建数据库名为tencent,集合名为tencent_zhaopin的集合操作对象

        i = 1
        while True:
            # 从 数据队列 取数据
            json_data = self.data_queue.get()
            dict = json.loads(json_data)

            self.collection.insert(dict)  # item对象先转为字典，再插入
            # 减一
            self.data_queue.task_done()

            print("爬取第%d条招聘" % i)
            i += 1

    def start_work(self):
        t_list = []

        t_url = threading.Thread(target=self.get_url_list)
        t_list.append(t_url)

        t_request = threading.Thread(target=self.send_request)
        t_list.append(t_request)

        t_detail_request = threading.Thread(target=self.detail_request)
        t_list.append(t_detail_request)

        t_analysic = threading.Thread(target=self.analysic)
        t_list.append(t_analysic)

        t_save = threading.Thread(target=self.save_data)
        t_list.append(t_save)

        # 线程守护 开启线程
        for t in t_list:
            t.setDaemon = True
            t.start()

        # 主线程等待
        for q in [self.url_queue, self.detail_url_queue, self.reaponse_queue, self.data_queue]:
            q.join()

    def run(self):

        start_time = time.time()

        # 执行任务
        self.start_work()

        end_time = time.time()

        print("总耗时:", end_time - start_time)
        print("总个数:", self.count)


if __name__ == '__main__':
    ZhiYouJi().run()
