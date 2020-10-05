"""
@Author  :   {Zixin Liu}
@Time    :   2020/10/4 14:16
@Contact :   {zixinliu@whu.edu.com}
@Desc    :  爱奇艺弹幕爬取复现
"""

import json
import zlib

import os
import pandas as pd
import requests
from bs4 import BeautifulSoup
import math
import glob
import multiprocessing


def get_IQ_data(tv_index, tv_id, duration):
    """由上文分析可知，只需要知道tvid，即可下载对应的弹幕压缩包"""
    url = 'https://cmts.iqiyi.com/bullet/{}/{}/{}_300_{}.z'
    datas = pd.DataFrame(columns=['uid', 'contentsId', 'contents', 'likeCount'])
    for index in range(1, math.ceil(duration / 300.0)):
        # https://cmts.iqiyi.com/bullet/视频编号的倒数4、3位/视频编号的倒数2、1位/视频编号_300_序号.z
        # 后分析发现，弹幕文件每5分钟（300秒）向服务器请求一次，故每集弹幕文件数量等于视频时间除以300之后向上取整，实际编程时这里可以简单处理
        myUrl = url.format(tv_id[-4:-2], tv_id[-2:], tv_id, index)
        # print(myUrl)
        res = requests.get(myUrl)
        if res.status_code == 200:
            btArr = bytearray(res.content)  # 需要解码
            xml = zlib.decompress(btArr).decode('utf-8')
            bs = BeautifulSoup(xml, "xml")  # 解析xml文档
            data = pd.DataFrame(columns=['uid', 'contentsId', 'contents', 'likeCount'])
            data['uid'] = [i.text for i in bs.findAll('uid')]  # 用户编号
            data['contentsId'] = [i.text for i in bs.findAll('contentId')]  # 弹幕对应ID
            data['contents'] = [i.text for i in bs.findAll('content')]  # 内容
            data['likeCount'] = [i.text for i in bs.findAll('likeCount')]  # 他人点赞该弹幕的数量
        else:
            break
        datas = pd.concat([datas, data], ignore_index=True)  # 一集中所有弹幕数据录入一个df，不设置ignore的话会出现index重复
    datas['tv_name'] = str(int(tv_index) + 1)  # 增加一列作为集数标识
    return datas


def get_TV_Id(aid):
    """每一集的tvid其实是由albumid生成的,具体Find - album找到info即可"""
    tv_id_list = []
    tv_duration_list = []
    tv_subtitle_list = []
    tv_description_list = []
    for page in range(1, 2):
        url = 'https://pcw-api.iqiyi.com/albums/album/avlistinfo?aid=' \
              + aid + '&page=' \
              + str(page) + '&size=30'
        res = requests.get(url).text
        res_json = json.loads(res)
        # 视频列表
        movie_list = res_json['data']['epsodelist']
        for j in movie_list:
            tv_id_list.append(j['tvId'])
            tv_description_list.append(j['description'])
            tv_subtitle_list.append(j['subtitle'])
            minutes, seconds = j['duration'].strip().split(':')
            tv_duration_list.append(int(minutes) * 60 + int(seconds))

    # 创建txt存储视频简介
    f1 = open("./bullet/description.txt", "w")
    for index, description in enumerate(tv_description_list):
        f1.write("想见你第{}集 {}\n".format(index + 1, description))
    f1.close()
    # 创建txt存储视频小标题
    f2 = open("./bullet/subtitle.txt", "w")
    for index, subtitle in enumerate(tv_subtitle_list):
        f2.write("想见你第{}集 {}\n".format(index + 1, subtitle))
    f2.close()

    return tv_id_list, tv_duration_list


def multip(duration_list, index, i):
    data = pd.DataFrame(get_IQ_data(index, str(i), duration_list[index]))
    data.to_csv('./bullet/' + str(index + 1) + '.csv')
    # data_all = pd.concat([data_all, data], ignore_index=True)  # 存入总表


if __name__ == '__main__':
    data_all = ["start flag"]
    # 想见你 album id
    my_aid = '248811101'  # 《在一起》aid: '4346635916604601'
    my_tv_id_list, my_tv_duration_list = get_TV_Id(my_aid)  # 获得电视剧集数，时长
    print("TV_ID列表：")
    print(my_tv_id_list)
    print("时长列表：")
    print(my_tv_duration_list)
    data_all = pd.DataFrame(columns=['uid', 'contentsId', 'contents', 'likeCount', 'tv_name'])  # 同样设置一个总表作为产出
    if not os.path.exists(r'.\bullet'):
        os.makedirs(r'.\bullet')

    multi_processing = []
    for index, i in enumerate(my_tv_id_list):
        # data = get_data('《想见你》 第'+index+'集',str(i))
        # 下一步改造成多进程
        # data = pd.DataFrame(get_IQ_data(index, str(i), my_tv_duration_list[index]))
        # data.to_csv('./bullet/' + str(index + 1) + '.csv')
        # data_all = pd.concat([data_all, data], ignore_index=True)  # 存入总表
        multi_processing.append(
            multiprocessing.Process(target=multip, args=(my_tv_duration_list, index, i)),
        )
    for t in multi_processing:
        # 启动进程
        t.start()
    for t in multi_processing:
        t.join()
    csv_list = glob.glob(r'.\bullet\*.csv')
    for data in csv_list:
        f = pd.read_csv(data)
        data_all = pd.concat([data_all, f], ignore_index=True)  # 存入总表
    del data_all['Unnamed: 0']

    print("{}集弹幕已经存入csv文件中".format(index + 1))
    data_all.to_csv(r'.\bullet\data_all.csv')
    print('总表已经合并完成，已存为csv')
