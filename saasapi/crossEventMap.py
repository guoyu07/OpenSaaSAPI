# coding: utf-8
from collections import OrderedDict
from pymongo import MongoClient
from config import mongo_ip
from config import mongo_port
import time
from MongoDatas import _sortByKey
from SaaSCommon.JHDecorator import fn_timer
from QueryOperator import query_operator

global conn

conn = MongoClient(mongo_ip, mongo_port)


# 线上漏斗（支持map）
@fn_timer
def crossEventMap(datatype, params):
    global conn
    try:
        tm_str_s = params["startDay"]
        tm_str_e = params["endDay"]
        events = params["events"]
        # 初始事件
        original_events = events[0]
        # 转化事件
        transform_events = events[1]
        last_num = int(params["windows"])
        tm_s_stamp = time.mktime(time.strptime(tm_str_s, "%Y-%m-%d"))
        tm_e_stamp = time.mktime(time.strptime(tm_str_e, "%Y-%m-%d"))
        attrs = params.get("attrs", {})
    except:
        return {"errinfo": "参数错误！"}

    if last_num > 60 or last_num < 0:
        return {"errinfo": "窗口期超出范围！"}

    num = (tm_e_stamp - tm_s_stamp)/86400
    if num > 60 or num < 0:
        return {"errinfo": "日期跨度超出范围！"}

    result = {}
    dbname = datatype
    collection_name = "uvfile"


    original_events_query = query_events(original_events)
    # transform_events_query = query_events(transform_events)

    query = []
    # 筛选
    match = {"$match": OrderedDict()}
    match["$match"].setdefault("tm", {"$gte": tm_str_s, "$lte": tm_str_e})
    # 版本
    if "jhd_vr" in attrs and isinstance(attrs["jhd_vr"], list):
        if len(attrs["jhd_vr"]) >= 1:
            attrs["jhd_vr"] = attrs["jhd_vr"][0]
        else:
            attrs.pop("jhd_vr")
    match["$match"].update(attrs)
    match["$match"].update(original_events_query)
    # 求出每天的筛选结果
    group = {"$group": {}}
    group["$group"].setdefault("_id", "$tm")
    group["$group"].setdefault("user_count", {"$sum": 1})
    group["$group"].setdefault("uid", {"$push": "$jhd_userkey"})
    # 按日期降序排序
    _sort = {"$sort": {"_id": -1}}
    query.append(match)
    query.append(group)
    query.append(_sort)
    # print original_events_query
    # print query
    query_result = [item for item in conn[dbname][collection_name].aggregate(query, allowDiskUse=True)]

    # 求出每一天留存事件
    step_results = map(lambda item: _map_func(item, last_num, datatype, transform_events), query_result)
    for item in step_results:
        key = item.keys()[0]
        result.setdefault(key, item[key])
    return _sortByKey(result)


def _map_func(item, last_num, datatype, events):
    global conn
    tm = item["_id"]
    step_result = {tm: [item.get("user_count", 0)]}
    tm_e = time.strftime("%Y-%m-%d", time.localtime(time.mktime(time.strptime(tm, "%Y-%m-%d"))+86400*last_num))
    query = OrderedDict()
    if last_num == 0:
        query.setdefault("tm", {"$gte": tm, "$lte": tm_e})
    else:
        query.setdefault("tm", {"$gt": tm, "$lte": tm_e})
    query.setdefault("jhd_userkey", {"$in": item["uid"]})
    transform_events_query = query_events(events)
    query.update(transform_events_query)
    query_result = conn[datatype]["uvfile"].distinct("jhd_userkey", query)
    del item["uid"]
    uv = len(query_result) if query_result else 0
    step_result[tm].append(uv)
    return step_result


def query_events(events):
    # events: [[{"id":"jhddg_every"},{"id":"jhddg_every"}]],
    # events: [[{"id":"ac8","attrs":[{"id":"id","op":"like","val":"135"}]}]]
    # 且关系
    and_relation = {"$and": []}
    for and_item in events:
        # and_item: [{"id":"jhddg_every"},{"id":"jhddg_every"}]
        # 或关系
        or_relation = {"$or": []}
        for or_item in and_item:
            event_attrs_query = {}
            # or_item: {"id":"ac8","attrs":[{"id":"id","op":"like","val":"135"}]}
            event_id = or_item["id"]
            if event_id == "jhddg_every":
                event_attrs_query.setdefault("item_count", {"$exists": True})
            else:
                if "attrs" in or_item and or_item["attrs"]:
                    for attr_item in or_item["attrs"]:
                        # 排除空值
                        if not attr_item:
                            continue
                        attr_id = attr_item["id"]
                        # 排除空值
                        if not attr_id:
                            continue
                        attr_value = attr_item["val"]
                        attr_op = attr_item["op"]
                        express = query_operator(attr_op, attr_value)
                        event_attrs_query.setdefault("item_count.%(event)s.maps.%(map_key)s" % {"event": event_id, "map_key": attr_id}, express)
                else:
                    event_attrs_query.setdefault("item_count.%(event)s" % {"event": event_id}, {"$exists": True})
            or_relation["$or"].append(event_attrs_query)
        and_relation["$and"].append(or_relation)
    return and_relation


if __name__ == "__main__":
    # print query_events([[{"id":"ac8","attrs":[{"id":"id","op":"like","val":"135"}, {"id":"id1","op":"eq","val":"135"}]},
    #                      {"id": "ac9","attrs": [{"id": "id", "op": "like", "val": "135"}, {"id": "id1", "op": "eq", "val": "135"}]}],
    #                     [{"id": "ac8",
    #                       "attrs": [{"id": "id", "op": "like", "val": "135"}, {"id": "id1", "op": "eq", "val": "135"}]}]])
    print crossEventMap("biqu", {"windows":7,"endDay":"2017-01-08","events":[[[{"id":"ac36"}]],[[{"id":"jhddg_every"}]]],"startDay":"2017-01-01"})


