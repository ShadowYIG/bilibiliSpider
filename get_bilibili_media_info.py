#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time       : 2020/5/28 9:15
# @Author     : ShadowY
# @File       : get_bilibili_media_info.py
# @Software   : PyCharm
# @Version    : 1.0
# @Description: 采用协程异步爬取b站番剧（经测试一分钟发送约7000次请求，
# 可爬取1000条左右的番剧信息，消耗ip量约为50个，batch不宜过大，30左右为宜，不能超过50），约4分钟能爬完b站所有番剧，3120部）
import random
import aiohttp
import asyncio
import pymongo
import logging
import time
import requests
from lxml import etree
import re
import json
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
UA_FILE = "user-agent.txt"
head = {
    'User-Agent': 'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/30.0.1599.17 Safari/537.36',
    'Referer': 'https://www.bilibili.com/'
    }
start_time = time.time()


def load_ua():
    """
    读取ua头，b站反爬策略恐怖，用大量ua达到每个请求不同ua
    :return:
    """
    uas = []
    with open(UA_FILE, 'rb') as uaf:
        for ua in uaf.readlines():
            if ua:
                uas.append(ua.strip()[:-1])  # 将请求头的user-agent加入到列表中
    random.shuffle(uas)  # 对列表中所有ua进行顺序打乱
    return uas


def get_proxy():
    """
    获取代理ip，虽然后来因为经常无法访问就注释掉了
    :return:
    """
    proxy_list = list()
    for i in range(1000):
        try:
            res = requests.get('http://118.24.52.95/get/').json()
            if res.get("proxy") not in proxy_list:
                if requests.get('https://www.bilibili.com/').status_code == 200:
                    proxy_list.append(res.get("proxy"))
            if len(proxy_list) >= 3:
                break
        except:
            time.sleep(60)
    return proxy_list


class GetMediaByAio:
    def __init__(self, page=1, batch=50, start=0, end=10):
        self.ua = load_ua()
        self.mongo = pymongo.MongoClient(host='localhost', port=27017)
        self.db = self.mongo['tkh']['bilibili_media']  # 存储数据的mongo集合
        self.err_db = self.mongo['tkh']['bilibili_media_err']
        self.finish = 0
        self.page = page
        self.batch = batch
        self.start_num = start
        self.end_num = end
        self.pre_time = time.time()
        self.media_list = list()
        # self.proxy = get_proxy()

    def get_media_list(self, start):
        """
        获取番剧列表
        :return:
        """
        page = round(start / self.batch) + 1
        self.page = page
        res = requests.get('https://api.bilibili.com/pgc/season/index/result?&page=%d&season_type=1&pagesize=%d&type=1' % (page, self.batch), headers=head).json()
        media_list = list()
        medias = res['data']['list']
        # 将每个番剧解析并加入到番剧列表中
        for media in medias:
            media_list.append({
                'link': media['link'],
                'media_id': media['media_id'],
                'title': media['title'],
                'season_id': media['season_id']
            })
        return media_list

    def callback(self, task):
        """
        信息获取的回调函数，用于存储信息
        :return:
        """
        page_text = task.result()
        if not page_text.get('_id'):  # 不存在视频id说明没爬取到数据，直接返回不需要存储
            return
        try:
            if self.db.find_one({"_id": page_text.get('_id')}):  # 查找数据库是否存在改id，如果存在则执行更新，否则添加
                if self.db.find_one({"_id": page_text.get('_id'), "intro": ''}):
                    self.db.update_one({"_id": page_text.get('_id')}, {'$set': page_text})
                    logging.info("Update finish: %d media_id: %d name:%s time: %.5f秒 TotalTime：%.5f秒" % (self.finish, page_text.get('_id'), page_text.get('title'), float(time.time() - self.pre_time), float(time.time() - start_time)))
            else:
                self.db.insert_one(page_text)  # 插入一行数据
                self.finish += 1
                # print("finish: %d mid: %d name:%s time: %.5f秒 TotalTime：%.5f秒" % (self.finish, page_text.get('_id'), page_text.get('name'), float(time.time()-self.pre_time), float(time.time()-start_time)))
                logging.info("finish: %d media_id: %d name:%s time: %.5f秒 TotalTime：%.5f秒" % (self.finish, page_text.get('_id'), page_text.get('title'), float(time.time() - self.pre_time), float(time.time() - start_time)))
        except pymongo.errors.DuplicateKeyError:
            logging.error("insert error mid: %d name:%s time: %.5f秒 TotalTime：%.5f秒" % (page_text.get('_id'), page_text.get('title'), float(time.time() - self.pre_time), float(time.time() - start_time)))
        self.pre_time = time.time()

    def start(self):
        start = self.start_num
        end = start + self.batch
        while start < self.end_num:  # 判断是否到达设定的数量
            self.media_list = self.get_media_list(start)  # 调用函数获取番剧列表
            print('当前进度为%d-%d' % (start, end))
            self.run(self.media_list)  # 将番剧列表传入多协程异步管理函数中进行异步获取详细信息
            start = end
            end += self.batch
            # self.proxy = get_proxy()

    async def get_media_info(self, media, proxy):
        """
        异步获取番剧信息
        :return:
        """
        headers = {
            'Origin': 'https://space.bilibili.com',
            'Referer': 'https://www.bilibili.com/',
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/30.0.1599.17 Safari/537.36'
        }
        myproxy = "http://{}".format(proxy)
        info_url = 'https://api.bilibili.com/pgc/review/user?media_id=%d' % media.get('media_id')
        episodes_url = 'https://api.bilibili.com/pgc/web/season/section?season_id=%d' % media.get('season_id')
        f_desc_url = 'https://www.bilibili.com/bangumi/media/md%d/' % media.get('media_id')  # 番剧的简介用vuessr出来的,在json找不到很神奇只能从页面获取
        relate_url = 'https://api.bilibili.com/pgc/review/relate?media_id=%d' % media.get('media_id')
        stat_url = 'https://api.bilibili.com/pgc/web/season/stat?season_id=%d' % media.get('season_id')
        # reply = 'https://api.bilibili.com/x/v2/reply?&jsonp=jsonp&pn=1&type=1&oid=242974042&sort=2'  # 获取评论的
        # desc_url = 'https://api.bilibili.com/x/web-interface/archive/desc?&aid='  # 番剧没有这东西，普通视频才有
        try:
            res_data = {}
            async with aiohttp.ClientSession(headers=headers) as session:
                # 获取番剧信息
                async with session.get(info_url, proxy=myproxy) as res:
                    if res.status != 200:  # 判断是否请求成功
                        logging.error("info访问网页失败 media_id:%d  status:%s" % (media.get('media_id'), res.status))
                    else:
                        info = await res.json()
                        info = info.get('result').get('media')
                        try:
                            info.get('areas')[0].get('name')
                        except IndexError:  # 出现数组越界说明其没有出产国家，直接利用try无视错误
                            logging.error("info area数组越界 media_id:%d  status:%s"% (media.get('media_id'), res.status))
                        res_data = {
                            '_id': info.get('media_id'),  # 番剧id
                            'media_id': info.get('media_id'),
                            'season_id': info.get('season_id'),
                            'title': info.get('title'),  # 番剧标题
                            'areas': info.get('areas')[0].get('name') if info.get('areas') else '无',  # 番剧出产国家
                            'cover': info.get('cover'),  # 声优信息
                            'count': info.get('rating').get('count') if info.get('rating') else '无',  # 评分人数
                            'score': info.get('rating').get('score') if info.get('rating') else '无',  # 评分
                            'type_name': info.get('type_name')  # 类别名称
                        }
                # 获取剧集列表
                async with session.get(episodes_url, proxy=myproxy) as res:
                    if res.status != 200:  # 判断是否请求成功
                        logging.error("episodes访问网页失败 media_id:%d  status:%s" % (media.get('media_id'), res.status))
                    else:
                        info = await res.json()
                        episodes = info.get('result').get('main_section').get('episodes')
                        ep_list = list()
                        for episode in episodes:
                            ep_list.append({
                                'aid': episode.get('aid'),  # id号
                                'cid': episode.get('cid'),  # id号
                                'badge': episode.get('badge'),  # 标识
                                'cover': episode.get('cover'),  # 声优
                                'id': episode.get('id'),  # id
                                'long_title': episode.get('long_title'),  # 长标题
                                'title': episode.get('title')  # 标题
                            })
                        res_data['episodes'] = ep_list
                # 获取番剧描述和作者
                async with session.get(f_desc_url, proxy=myproxy) as res:
                    if res.status != 200:  # 判断是否请求成功
                        logging.error("f_desc访问网页失败 media_id:%d  status:%s" % (media.get('media_id'), res.status))
                    else:
                        html = await res.text(encoding="utf-8")
                        js = re.search('__INITIAL_STATE__=(.*?);\(', html).group(1)  # 由于b站可能使用vue-ssr框架编写的描述信息在页面中，利用正则表达式去匹配
                        info = json.loads(js)  # 解析获取到的json
                        res_data['actors'] = info.get('mediaInfo').get('actors')  # 作者
                        res_data['intro'] = info.get('mediaInfo').get('evaluate')  # 描述
                # 获取番剧长短评(仅获取长评就够了）
                async with session.get(relate_url, proxy=myproxy) as res:
                    if res.status != 200:  # 判断是否请求成功
                        logging.error("relate访问网页失败 media_id:%d  status:%s" % (media.get('media_id'), res.status))
                    else:
                        info = await res.json()
                        long_reviews = info.get('data').get('long_review')  # 长评
                        review = list()
                        for long_review in long_reviews:
                            title = long_review.get('title')
                            async with session.get(long_review.get('url'), proxy=myproxy) as v_res:
                                html = await v_res.text()
                                content = ''.join(etree.HTML(html).xpath('//div[@class="article-holder"]//text()'))  # 使用etree去匹配
                                review.append([title, content])
                        res_data['long_review'] = review
                # 获取start
                async with session.get(stat_url, proxy=myproxy) as res:
                    if res.status != 200:  # 判断是否请求成功
                        logging.error("stat访问网页失败 media_id:%d  status:%s" % (media.get('media_id'), res.status))
                    else:
                        info = await res.json()
                        info = info.get('result')
                        res_data['coins'] = info.get('coins')  # 投币数
                        res_data['danmakus'] = info.get('danmakus')  # 弹幕量
                        res_data['series_follow'] = info.get('series_follow')  # 追番人数
                        res_data['views'] = info.get('views')  # 观看人数
            return res_data
        except(aiohttp.ClientError, asyncio.TimeoutError, aiohttp.client_exceptions.ClientConnectorError):
            self.err_db.insert_one({'media_id': media.get('media_id'), 'page': self.page, 'type': '代理错误'})
            logging.error("代理/网络错误 media_id:%d" % media.get('media_id'))

    async def main(self, media_list):
        """
        负责加入协程任务的函数
        :param media_list:
        :return:
        """
        tasks = list()
        async with asyncio.Semaphore(500):
            for media in media_list:
                # task = asyncio.ensure_future(self.getsource(mid, random.choice(self.proxy)))
                task = asyncio.ensure_future(self.get_media_info(media, ''))  # 创建任务
                task.add_done_callback(self.callback)  # 添加回调
                tasks.append(task)  # 加入任务
            await asyncio.wait(tasks)

    def run(self, media_list):
        """
        协程总负责
        :param media_list:
        :return:
        """
        loop = asyncio.new_event_loop()  # 创建事件循环对象
        asyncio.set_event_loop(loop)  # 将对象加入协程
        loop.run_until_complete(self.main(media_list))  # 激活协程
        loop.run_until_complete(asyncio.sleep(0))
        loop.close()  # 关闭事件循环


if __name__ == '__main__':
    a = GetMediaByAio(page=50, batch=30, start=1500, end=3120)
    a.start()
