#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# @Time       : 2020/5/27 14:01
# @Author     : ShadowY
# @File       : get_bilibili_info.py
# @Software   : PyCharm
# @Version    : 1.0
# @Description: None

import random
import aiohttp
import asyncio
import pymongo
import logging
import time
import requests
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(filename)s[line:%(lineno)d] - %(levelname)s: %(message)s')
UA_FILE = "user-agent.txt"
head = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/30.0.1599.17 Safari/537.36',
        'Referer': 'https://space.bilibili.com/14568909?from=search&seid=' + str(random.randint(10000, 50000))
    }
start_time = time.time()


def load_ua():
    uas = []
    with open(UA_FILE, 'rb') as uaf:
        for ua in uaf.readlines():
            if ua:
                uas.append(ua.strip()[:-1])
    random.shuffle(uas)
    return uas


def get_proxy():
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


class GetInfoByAio:
    def __init__(self, syn_c=20, start=0, end=10):
        self.ua = load_ua()
        self.mongo = pymongo.MongoClient(host='localhost', port=27017)
        self.db = self.mongo['tkh']['bilibili_user']  # 存储数据的mongo集合
        self.err_db = self.mongo['tkh']['bilibili_err']
        self.syn_c = syn_c
        self.finish = 0
        self.start_num = start
        self.end_num = end
        self.pre_time = time.time()
        # self.proxy = get_proxy()
        # print(self.proxy)

    def start(self):
        start = self.start_num
        end = start + self.syn_c
        while start < self.end_num:  # 300000000:  # 3亿
            print('当前进度为%d-%d' % (start, end))
            self.err_db.find()
            self.run(range(start, end))
            start = end
            end += self.syn_c
            # self.proxy = get_proxy()

    def callback(self, task):
        # 获取响应数据
        page_text = task.result()
        if not page_text.get('_id'):
            return
        try:
            self.db.insert_one(page_text)
            self.finish += 1
            # print("finish: %d mid: %d name:%s time: %.5f秒 TotalTime：%.5f秒" % (self.finish, page_text.get('_id'), page_text.get('name'), float(time.time()-self.pre_time), float(time.time()-start_time)))
            logging.info("finish: %d mid: %d name:%s time: %.5f秒 TotalTime：%.5f秒" % (self.finish, page_text.get('_id'), page_text.get('name'), float(time.time()-self.pre_time), float(time.time()-start_time)))
        except pymongo.errors.DuplicateKeyError:
            logging.error("insert error mid: %d name:%s time: %.5f秒 TotalTime：%.5f秒" % (page_text.get('_id'), page_text.get('name'), float(time.time()-self.pre_time),float(time.time()-start_time)))
        self.pre_time = time.time()

    async def get_user_info(self, mid, proxy):
        headers = {
            'Host': 'api.bilibili.com',
            'Origin': 'https://space.bilibili.com',
            'Referer': 'https://space.bilibili.com/%d' % mid,
            'User-Agent': 'Mozilla/5.0 (Windows NT 6.2; WOW64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/30.0.1599.17 Safari/537.36'
        }
        myproxy = "http://{}".format(proxy)
        info_url = 'http://api.bilibili.com/x/space/acc/info?mid=%d&jsonp=jsonp' % mid
        info_url1 = 'http://space.bilibili.com/ajax/member/GetInfo'  # 通过js发现的第二个获取地址post请求
        stat_url = 'http://api.bilibili.com/x/relation/stat?vmid=%d&jsonp=jsonp' % mid
        upstat_url = 'http://api.bilibili.com/x/space/upstat?mid=%d&jsonp=jsonp' % mid
        data = {}
        try:
            async with aiohttp.ClientSession(headers=headers) as session:
                payload = {
                    'mid': mid
                }
                async with session.request('GET', info_url, data=payload, proxy=myproxy) as resp:
                    if resp.status != 200:
                        logging.error("info访问网页失败 mid:%d  status:%s" % (mid, resp.status))
                    else:
                        info = await resp.json()
                        info = info.get('data')
                        if info:
                            data.update({
                                '_id': info['mid'],
                                'name': info['name'],
                                'sex': info['sex'],
                                'face': info['face'],
                                'rank': info['rank'],
                                'vip': ('会员' if info['vip']['type'] == 1 else '大会员') if info['vip']['status'] == 1 else '无',
                                'sign': info['sign'],
                                'level': info['level'],
                                'birthday': info['birthday'],
                                'coins': info['coins']
                            })
                async with session.request('GET', stat_url, data=payload, proxy=myproxy) as resp:
                    if resp.status == 200:
                        stat = await resp.json()
                        if stat:
                            stat = stat.get('data')
                            if stat:
                                data.update({'following': stat.get('following'), 'follower': stat.get('follower')})
                    else:
                        logging.error("stat_url访问网页失败 mid:%d  status:%d" % (mid, resp.status))
                async with session.request('GET', upstat_url, data=payload, proxy=myproxy) as resp:
                    if resp.status == 200:
                        stat = await resp.json()
                        if stat:
                            stat = stat.get('data')
                            if stat:
                                data.update({"archive": stat["archive"]["view"], "article": stat["article"]["view"]})
                    else:
                        logging.error("upstat_url访问网页失败 mid:%d  status:%d" % (mid, resp.status))
        except(aiohttp.ClientError, asyncio.TimeoutError, aiohttp.client_exceptions.ClientConnectorError):
            self.err_db.insert_one({'mid': mid})
            logging.error("代理/网络错误 mid:%d" % mid)
        return data

    async def main(self, mid_iterator):
        tasks = list()
        async with asyncio.Semaphore(500):
            for mid in mid_iterator:
                # task = asyncio.ensure_future(self.getsource(mid, random.choice(self.proxy)))
                task = asyncio.ensure_future(self.get_user_info(mid, ''))
                task.add_done_callback(self.callback)  # 添加回调
                tasks.append(task)
            await asyncio.wait(tasks)

    def run(self, mid_iterator):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self.main(mid_iterator))  # 激活协程
        loop.run_until_complete(asyncio.sleep(0))
        loop.close()  # 关闭事件循环


if __name__ == '__main__':
    a = GetInfoByAio(syn_c=50, start=3544, end=1000000)
    start_time = time.time()
    a.start()
