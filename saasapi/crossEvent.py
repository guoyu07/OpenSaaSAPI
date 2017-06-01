# coding: utf-8
from pymongo import MongoClient
from config import mongo_ip
from config import mongo_port
import time
from MongoDatas import _sortByKey
from SaaSCommon.JHDecorator import fn_timer

global conn

conn = MongoClient(mongo_ip, mongo_port)


# 线上漏斗（不支持map）
@fn_timer
def eventsCrossCombine(datatype, tm_str_s, tm_str_e, last_num, events = []):
    global conn

    tm_s_stamp = time.mktime(time.strptime(tm_str_s, "%Y-%m-%d"))
    tm_e_stamp = time.mktime(time.strptime(tm_str_e, "%Y-%m-%d"))
    num = (tm_e_stamp - tm_s_stamp)/86400
    if num > 90 or num <= 0:
        return ["Out of date range"]

    dbname = datatype
    collection_name = "uvfile"
    result = {}
    query = []
    # 筛选
    match = {"$match": {}}
    match["$match"].setdefault("tm", {"$gte": tm_str_s, "$lte": tm_str_e})
    if events[0] != "jhddg_every":
        match["$match"].setdefault("item_count.%s"%events[0], {"$exists": True})
    # 求出每天的筛选结果
    group = {"$group": {}}
    group["$group"].setdefault("_id", "$tm")
    group["$group"].setdefault(events[0], {"$sum": 1})
    group["$group"].setdefault("uid", {"$push": "$jhd_userkey"})
    # 按日期降序排序
    _sort = {"$sort": {"_id": -1}}
    query.append(match)
    query.append(group)
    query.append(_sort)
    query_result = [item for item in conn[dbname][collection_name].aggregate(query, allowDiskUse=True)]

    # 求出每一天留存事件
    step_results = map(lambda item: _map_func(item, last_num, datatype, events), query_result)
    for item in step_results:
        key = item.keys()[0]
        result.setdefault(key, item[key])
    return _sortByKey(result)


def _map_func(item, last_num, datatype, events):
    global conn
    query = {}
    tm = item["_id"]
    event_0 = events[0]
    event_1 = events[1]
    step_result = {tm: [item.get(event_0, 0)]}
    tm_e = time.strftime("%Y-%m-%d", time.localtime(time.mktime(time.strptime(tm, "%Y-%m-%d"))+86400*last_num))
    query.setdefault("tm", {"$gt": tm, "$lte": tm_e})
    query.setdefault("jhd_userkey", {"$in": item["uid"]})
    if events[1] != "jhddg_every":
        query.setdefault("item_count.%s"%events[1], {"$exists": True})
    query_result = conn[datatype]["uvfile"].distinct("jhd_userkey", query)
    del item["uid"]
    uv = len(query_result) if query_result else 0
    step_result[tm].append(uv)
    return step_result


if __name__ == "__main__":
    print eventsCrossCombine("biqu", "2016-09-20", "2016-10-27", 3, ["ac21", "ac27"])


