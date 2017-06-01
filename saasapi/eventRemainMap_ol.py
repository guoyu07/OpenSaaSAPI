# coding: utf-8
import time
import datetime
import threading
from collections import OrderedDict
from pymongo import MongoClient
from config import mongo_ip
from config import mongo_port
from MongoDatas import _sortByKey
from crossEventMap import query_events
from SaaSCommon.JHDecorator import fn_timer

global conn

conn = MongoClient(mongo_ip, mongo_port)


@fn_timer
# def eventsRemainMap(datatype, tm_s, tm_e, last_num, events):
def eventsRemainMap(datatype, params):
    print params
    try:
        tm_s = params["startDay"]
        tm_e = params["endDay"]
        last_num = params["remain"]
        events = params["events"]
    except:
        return {"errinfo": "参数错误！"}

    tm_s_stamp = time.mktime(time.strptime(tm_s, "%Y-%m-%d"))
    tm_e_stamp = time.mktime(time.strptime(tm_e, "%Y-%m-%d"))
    num = (tm_e_stamp - tm_s_stamp)/86400
    if num > 60 or num <= 0:
        return {"errinfo": "超出范围！"}

    if last_num >= 90 or last_num <= 0:
        return {"errinfo": "超出范围！"}

    tm = datetime.datetime.strptime(tm_s, "%Y-%m-%d")
    end_tm = datetime.datetime.strptime(tm_e, "%Y-%m-%d")
    today = datetime.datetime.today()
    result = []
    threads = []
    while tm <= end_tm:
        t = threading.Thread(target=thread_func, args=(datatype, tm.strftime("%Y-%m-%d"), last_num, today, events, result))
        t.start()
        tm += datetime.timedelta(days=1)
        threads.append(t)

        # tmp = {}
        # tmp.setdefault("tm", tm.strftime("%Y-%m-%d"))
        # # 计算留存数据，返回数据格式：{日期： 回访人数}，按日期升序
        # event_remain_data = eventsRemain(datatype, tm.strftime("%Y-%m-%d"), last_num, events)
        # data_default = {}
        # # 生成返回数据，没有回访行为的日期补0，没有数据的日志补空字符
        # for i in range(last_num+1):
        #     curday = (tm+datetime.timedelta(days=i)).strftime("%Y-%m-%d")
        #     data_default.setdefault(curday, 0 if today.strftime("%Y-%m-%d") > curday else "")
        # data_keys = data_default.keys()
        # data_keys.sort()
        # # 回去返回日期的留存数据
        # for key in data_keys:
        #     tmp.setdefault("numbers", [])
        #     tmp["numbers"].insert(len(tmp["numbers"]), event_remain_data[key] if key in event_remain_data else data_default[key])
        # tmp["length"] = len(tmp["numbers"])
        # tm += datetime.timedelta(days=1)
        # result.append(tmp)

    for t in threads:
        t.join()
    result_sorted = sorted(result, key=lambda item: item["tm"], reverse=False)
    return result_sorted


def thread_func(datatype, tm, last_num, today, events, result):
    tmp = {}
    tmp.setdefault("tm", tm)
    # 计算留存数据，返回数据格式：{日期： 回访人数}，按日期升序
    event_remain_data = eventsRemain(datatype, tm, last_num, events)
    data_default = {}
    # 生成返回数据，没有回访行为的日期补0，没有数据的日志补空字符
    for i in range(last_num + 1):
        curday = (datetime.datetime.strptime(tm, "%Y-%m-%d") + datetime.timedelta(days=i)).strftime("%Y-%m-%d")
        data_default.setdefault(curday, 0 if today.strftime("%Y-%m-%d") > curday else "")
    data_keys = data_default.keys()
    data_keys.sort()
    # 回去返回日期的留存数据
    for key in data_keys:
        tmp.setdefault("numbers", [])
        tmp["numbers"].insert(len(tmp["numbers"]),
                              event_remain_data[key] if key in event_remain_data else data_default[key])
    tmp["length"] = len(tmp["numbers"])
    # tm += datetime.timedelta(days=1)
    result.append(tmp)


def eventsRemain(datatype, tm_0, last_num, events):
    global conn
    assert last_num >= 1
    assert type(events) == type([]) and len(events) >= 1
    today = datetime.datetime.today()
    start_event = events[0]
    # format: [[{id:…,attrs:[{id:…,op:…,val:…},{其他mapkey条件}]},{或者关系}], [{并且关系}]
    remain_event = events[1]
    dbname = datatype
    collection_name = "uvfile"
    # 如果不是所有事件，则添加限制条件
    query = query_events(start_event)
    query.setdefault("tm", tm_0)
    qurey_result = list(conn[dbname][collection_name].find(query, {"jhd_userkey": 1}))
    uids = tuple([item["jhd_userkey"] for item in qurey_result])
    tm_s = (datetime.datetime.strptime(tm_0, "%Y-%m-%d") + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
    end_day = datetime.datetime.strptime(tm_0, "%Y-%m-%d") + datetime.timedelta(days=last_num)
    tm_e = end_day.strftime("%Y-%m-%d")
    tm_e = min(tm_e, (today-datetime.timedelta(days=1)).strftime("%Y-%m-%d"))
    events_remain = eventIntersection(dbname, tm_s, tm_e, uids, remain_event)
    result = {}
    result.setdefault(tm_0, len(uids))
    for item in events_remain:
        tm = item["_id"]
        del item["_id"]
        eventid = item.keys()[0]
        result.setdefault(tm, item[eventid])
    return _sortByKey(result)


def eventIntersection(datatype, tm_s, tm_e, uids, event):
    global conn
    query = []
    # 根据留存天数获取期间的活跃用户
    match = {"$match": OrderedDict()}
    match["$match"].setdefault("tm", {"$gte": tm_s, "$lte": tm_e})
    match["$match"].setdefault("jhd_userkey", {"$in": uids})
    event_query = query_events(event)
    match["$match"].update(event_query)
    # 计算每日留存
    group = {"$group": {}}
    group["$group"].setdefault("_id", "$tm")
    group["$group"].setdefault("event", {"$sum": 1})
    _sort = {"$sort": {"_id": -1}}

    query.append(match)
    query.append(group)
    query.append(_sort)
    query_result = [item for item in conn[datatype]["uvfile"].aggregate(query, allowDiskUse=True)]
    return query_result


if __name__ == "__main__":
    import time
    a = time.time()

    print eventsRemainMap("biqu", {"remain":20,"endDay":"2017-01-08","events":[[[{"id":"ac36"}]],[[{"id":"jhddg_every"}]]],"startDay":"2017-01-01"})
    # for i in range(1, 7):
    #     tm_s = (datetime.datetime.strptime("2016-09-01", "%Y-%m-%d") + datetime.timedelta(days=i)).strftime("%Y-%m-%d")
    #     print eventsRemain("feeling", tm_s, 15, ["ac11", "ac23"])
    # print time.time()-a
