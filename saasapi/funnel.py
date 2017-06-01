# -*- coding: utf-8 -*-
import datetime
import time
from collections import OrderedDict
from QueryOperator import query_operator
from pymongo import MongoClient
from config import mongo_ip
from config import mongo_port

from MongoDatas import _sortByKey
from SaaSCommon.JHDecorator import fn_timer

# from config import mongo_con_string
# import json

global conn

conn = MongoClient(mongo_ip, mongo_port)


# 线上漏斗（不支持map）
@fn_timer
def funneldataOld(datatype, s_tm, e_tm, events):
    # 保留旧接口，不支持map属性查询
    '''
    :param datatype:
    :param s_tm:
    :param e_tm:
    :param events:
    :return:
    :desc: 任意事件id： jhd_every
    '''
    dbname = datatype
    collection_name = "uvfile"
    s_stamp = time.mktime(time.strptime(s_tm, "%Y-%m-%d"))
    e_stamp = time.mktime(time.strptime(e_tm, "%Y-%m-%d"))
    result = {}
    # events = map(str, events)
    while s_stamp <= e_stamp:
        _tm = time.strftime("%Y-%m-%d", time.localtime(s_stamp))
        result.setdefault(_tm, [])
        for i in range(0, len(events)):
            query = []
            match = {"$match": {"tm": _tm}}
            for event in events[0:i+1]:
                if type(event) == type([]) and len(event) == 1:
                    event =event[0]
                    if event == "jhddg_every":
                        match["$match"].setdefault("item_count", {"$exists": True})
                    else:
                        match["$match"].setdefault("item_count.%s"%event, {"$exists": True})
                # 兼容旧接口格式
                elif type(event) == type("") or type(event) == type(u""):
                    event = event
                    if event == "jhddg_every":
                        match["$match"].setdefault("item_count", {"$exists": True})
                    else:
                        match["$match"].setdefault("item_count.%s"%event, {"$exists": True})
                elif type(event) == type([]) and len(event) > 1:
                    value = event
                    tmp = {"$or": []}
                    for _key in value:
                        if _key == "jhddg_every":
                            tmp["$or"].append({"item_count": {"$exists": True}})
                        else:
                            tmp["$or"].append({"item_count.%s" % _key: {"$exists": True}})
                    match["$match"].setdefault("$and", []).append(tmp)
            query.append(match)
            project = {"$project": {}}
            for event in events[0:i+1]:
                if type(event) == type([]) and len(event) == 1:
                    event = event[0]
                    if event == "jhddg_every":
                        project["$project"].setdefault("jhd_userkey", 1)
                    else:
                        project["$project"].setdefault("jhd_userkey", 1)
                        project["$project"].setdefault("item_count.%s.opatm"%event, 1)
                elif type(event) == type("") or type(event) == type(u""):
                    event = event
                    if event == "jhddg_every":
                        project["$project"].setdefault("jhd_userkey", 1)
                    else:
                        project["$project"].setdefault("jhd_userkey", 1)
                        project["$project"].setdefault("item_count.%s.opatm"%event, 1)
                elif type(event) == type([]) and len(event) > 1:
                    value = event
                    [project["$project"].setdefault("item_count.%s.opatm"%_key, 1) for _key in value]
            query.append(project)
            # group = {"$group": {"_id": "all", "count": {"$sum": 1}, "users": {"$push": "$jhd_userkey"}}}
            group = {"$group": {"_id": "all", "count": {"$sum": 1}}}
            query.append(group)
            for item in conn[dbname][collection_name].aggregate(query, allowDiskUse=True):
                result[_tm].append(item["count"])
            if len(result[_tm]) != i+1:
                result[_tm].append(0)
        s_stamp += 86400
    return _sortByKey(result)


def operator_query(op, value):
    if op == "is":
        return value
    if op == "nis":
        return {"$ne": str(value)}
    elif op == "like":
        return {"$regex": str(value)}
    elif op == "nlike":
        return value
    elif op == "startswith":
        return {"$regex": "^"+str(value)}
    elif op == "endswith":
        return {"$regex": str(value)+"$"}

    elif op == "eq":
        return int(value)
    elif op == "ne":
        return {"$ne": int(value)}
    elif op == "lte":
        return {"$lte": int(value)}
    elif op == "lt":
        return {"$lt": int(value)}
    elif op == "gte":
        return {"$gte": int(value)}
    elif op == "gt":
        return {"$gt": int(value)}
    elif op == "in":
        return {"$in": int(value)}

    raise NotImplementedError("operator %s is invalid!" % (op, ))

# 线上漏斗（支持map）
@fn_timer
def funneldata(datatype, s_tm, e_tm, params, cycle = "daily"):
    # 保留旧接口，不支持map属性查询
    '''
    :param datatype:
    :param s_tm:
    :param e_tm:
    :param events:
    :return:
    :desc: 任意事件id： jhd_every
    '''
    # operators for string: is, like, nlike, startswith, endswith
    # operators for number: eq, neq, lte, lt, gte, gt

    dbname = datatype
    collection_name = "uvfile"
    s_stamp = time.mktime(time.strptime(s_tm, "%Y-%m-%d"))
    e_stamp = time.mktime(time.strptime(e_tm, "%Y-%m-%d"))

    num = (e_stamp - s_stamp)/86400
    if num > 60 or num < 0:
        return {"errinfo": "日期跨度超出范围！"}

    eventsdata = params["funnel"]
    # funnel format: [[{"id": "ac41", "map": {"og": "CSX"}}], [{"id": "ac23", "map": {}}, {"id": "ac11", "map": {}}], [{"id": "ac22", "map": {}}]]
    events = []
    events_seq_map = {}
    for step, item in enumerate(eventsdata):
        tmp = []
        for step_part, event in enumerate(item):
            tmp.append(event["id"])
            events_seq_map.setdefault(step, {}).setdefault(step_part, {}).setdefault(event["id"], event.get("attrs", {}))
        events.append(tmp)
    attrs = params.get("attrs", {})
    if "jhd_vr" in attrs and isinstance(attrs["jhd_vr"], list):
        if len(attrs["jhd_vr"]) >= 1:
            attrs["jhd_vr"] = attrs["jhd_vr"][0]
        else:
            attrs.pop("jhd_vr")

    result = {}
    # events = map(str, events)
    while s_stamp <= e_stamp:
        _tm = time.strftime("%Y-%m-%d", time.localtime(s_stamp))
        result.setdefault(_tm, [])
        for i in range(0, len(events)):
            query = []
            match = {"$match": {"tm": _tm}}
            match["$match"].update(attrs)
            for step, event in enumerate(events[0:i+1]):
                if type(event) == type([]):
                    value = event
                    tmp = OrderedDict([
                        ("$or", [])
                    ])
                    for step_part, _key in enumerate(value):
                        event_mapkeys = events_seq_map[step][step_part][_key]
                        if not _key:
                            continue
                        if _key == "jhddg_every":
                            tmp["$or"].append({"item_count": {"$exists": True}})
                        else:
                            map_conds = {}
                            for map_key in event_mapkeys:
                                id_key = map_key["id"]
                                if not id_key:
                                    continue
                                id_value = map_key["val"]
                                op = map_key["op"]
                                try:
                                    express = query_operator(op, id_value)
                                except:
                                    return {"errinfo": "类型错误！"}
                                map_conds.setdefault("item_count.%(event)s.maps.%(map_key)s" % {"event": _key, "map_key": id_key}, express)
                            if map_conds:
                                tmp["$or"].append(map_conds)
                            else:
                                tmp["$or"].append({"item_count.%s" % _key: {"$exists": True}})
                    if tmp["$or"]:
                        match["$match"].setdefault("$and", []).append(tmp)
            # print json.dumps(match)
            query.append(match)
            project = {"$project": {}}
            for event in events[0:i+1]:
                if type(event) == type([]) and len(event) == 1:
                    event = event[0]
                    if not event:
                        continue
                    if event == "jhddg_every":
                        project["$project"].setdefault("jhd_userkey", 1)
                    else:
                        project["$project"].setdefault("jhd_userkey", 1)
                        project["$project"].setdefault("item_count.%s.opatm"%event, 1)
                elif type(event) == type("") or type(event) == type(u""):
                    event = event
                    if not event:
                        continue
                    if event == "jhddg_every":
                        project["$project"].setdefault("jhd_userkey", 1)
                    else:
                        project["$project"].setdefault("jhd_userkey", 1)
                        project["$project"].setdefault("item_count.%s.opatm"%event, 1)
                elif type(event) == type([]) and len(event) > 1:
                    value = event
                    if not event:
                        continue
                    [project["$project"].setdefault("item_count.%s.opatm"%_key, 1) for _key in value]
            if project["$project"]:
                query.append(project)
            # group = {"$group": {"_id": "all", "count": {"$sum": 1}, "users": {"$push": "$jhd_userkey"}}}
            group = {"$group": {"_id": "all", "count": {"$sum": 1}}}
            query.append(group)
            # import json
            # print json.dumps(query)
            for item in conn[dbname][collection_name].aggregate(query, allowDiskUse=True):
                result[_tm].append(item["count"])
            if len(result[_tm]) != i+1:
                result[_tm].append(0)
        s_stamp += 86400
    return _sortByKey(result)


def group_by_week(data):
    # data format
    #{
    # "2016-06-17": [
    #     1717,
    #     1105,
    #     1105],
    # "2016-06-16": [
    #     1717,
    #     1105,
    #     1105],
    # }

    # Monday is 0 and Sunday is 6
    result = {}
    days = data.keys()
    min_day = datetime.datetime.strptime(min(days), "%Y-%m-%d")
    min_day_tumple = min_day.timetuple()
    min_day_weekday = min_day.isoweekday()
    max_day = datetime.datetime.strptime(max(days), "%Y-%m-%d")
    max_day_tumple = max_day.timetuple()
    max_day_weekday = min_day.isoweekday()

    week_start_day = (min_day - (min_day_weekday - datetime.timedelta(days=1))).strftime("%Y-%m-%d")


def group_by_month(result):
    pass


@fn_timer
def funneldata_bak(datatype, s_tm, e_tm, params):
    start_day = datetime.datetime.strptime(s_tm, "%Y-%m-%d")
    end_day = datetime.datetime.strptime(e_tm, "%Y-%m-%d")
    day_span = (end_day - start_day).days
    if day_span > 30:
        start_day = end_day - 30
    eventsdata = params["funnel"]
    attrs = params.get("attrs", {})
    result = OrderedDict()
    while start_day <= end_day:
        tm = end_day.date().strftime("%Y-%m-%d")
        for i in range(1, len(eventsdata)+1):
            eventsdata_part = eventsdata[:i]
            funnel_count = funnelstep(datatype, tm, eventsdata_part, attrs=attrs)
            result.setdefault(tm, []).append(funnel_count)
        end_day = end_day - datetime.timedelta(days=1)
        # print(result)
    return result


# 根据筛选出的
def funnelstep(datatype, tm, eventsdata, attrs = None):
    '''
    :param datatype:
    :param tm:
    :param eventsdata: [[{"id": "ac41", "map": {"og": "CSX"}}], [{"id": "ac23", "map": {}}, {"id": "ac11", "map": {}}], [{"id": "ac22", "map": {}}]]
    :return:
    '''
    if not attrs:
        attrs = {}
    tm_short = tm.replace("-", "")
    dbname = datatype
    collection_name = "UserEvent"

    events_steps = [] #　[["in"], ["ac23", "ac11"], ["ac22"]]

    event_group_condition = {}
    for events_step in eventsdata:
        event_onestep = []
        for item in events_step:
            eventid = item["id"]
            event_onestep.append(eventid)
            # 生成 map 筛选条件
            # if item["map"]: # 排除空 map 的情况
            event_group_condition.setdefault(eventid, []).append(item.get("map", {}))
        events_steps.append(event_onestep)
    events_num = len(events_steps)
    raw_data = funnelstepRawData(datatype, tm_short, events_steps, attrs)

    # print(raw_data)

    # 如果第一步筛选结果数为 0，则直接返回 0
    if raw_data[tm]["count"] == 0:
        return 0

    # 如果是第一步漏斗则直接返回结果；如果第一步为 “任意事件” 则第二步漏斗时直接返回结果
    if len(eventsdata) == 1 or (len(eventsdata) == 2 and eventsdata[0][0]["id"] == "jhddg_every"):
        return raw_data[tm]["count"]

    query = []
    users = raw_data[tm]["users"]
    # events = itertools.chain(reduce(operator.concat, events_steps, [])) # 生成 events 列表
    match = {"$match": OrderedDict([("partition_date", tm_short), ("jhd_userkey", {"$in": users})])}
    or_part = {"$or": []}
    for _eventid in event_group_condition:
        if _eventid == "jhddg_every":
            continue
        user_map = event_group_condition[_eventid]
        for map_s in user_map:
            tmp = {}
            tmp["jhd_eventId"] = _eventid
            for key in map_s:
                tmp["jhd_map.%s"%key] = map_s[key]
        or_part["$or"].append(tmp)
    match["$match"].update(**or_part)
    sort_by = {"$sort": {"jhd_ts": 1}}
    group = {"$group": {"_id": "$jhd_userkey", "events": {"$push": "$jhd_eventId"}, "ts": {"$push": "$jhd_ts"}, "eventset": {"$addToSet": "$jhd_eventId"}}}
    # 计算事件漏斗个数
    project = {"$project": {"events": 1, "_id": 0, "ts": 1, "event_size": {"$size": "$eventset"}}}
    # 排除掉不符合漏斗事件个数要求的数据
    match_1 = {"$match": {"event_size": events_num}}

    query.append(match)
    # query.append(sort_by)
    query.append(group)
    query.append(project)
    query.append(match_1)
    import json
    # print("funnelstep query", json.dumps(query))
    funnel_count = 0

    for item in conn[dbname][collection_name].aggregate(query, allowDiskUse=True):
        ts = item["ts"]
        user_events = item["events"]
        # 用户事件按操作时间升序排序
        events_ts = dict([(_ts, _event) for _ts, _event in zip(ts, user_events)])
        ts.sort()
        user_events = [events_ts[_ts] for _ts in ts]
        for events_onestep, step_index in zip(events_steps, range(0, len(events_steps))):
            iscontinue = True
            # 根据用户触发事件序列计算漏斗事件
            for eventid in events_onestep:
                try:
                    slice_index = user_events.index(eventid)
                    user_events = user_events[slice_index:]
                    break
                except ValueError:
                    iscontinue = False
                    break
            if not iscontinue:
                break
        funnel_count += 1
    return funnel_count

# uvfile中筛选出用户 userkey
def funnelstepRawData(datatype, tm, events, attrs = None):
    '''
    :param datatype:
    :param s_tm:
    :param e_tm:
    :param events:
    :return:
    :desc: 任意事件id： jhd_every
    '''
    if attrs == None:
        attrs = {}
    dbname = datatype
    collection_name = "uvfile"
    time_stamp = time.mktime(time.strptime(tm, "%Y%m%d"))
    result = {}
    # events = map(str, events)
    _tm = time.strftime("%Y-%m-%d", time.localtime(time_stamp))
    # result.setdefault(_tm, [])
    for i in [max(range(0, len(events)))]:
        query = []
        match = {"$match": OrderedDict([("tm", _tm)])}
        for attr in attrs:
            if not isinstance(attrs[attr], list):
                continue
            match["$match"].setdefault(attr, {"$in": attrs[attr][:10]})
        for event in events[0:i+1]:
            if type(event) == type([]) and len(event) == 1:
                event = event[0]
                if event == "jhddg_every":
                    match["$match"].setdefault("item_count", {"$exists": True})
                else:
                    match["$match"].setdefault("item_count.%s"%event, {"$exists": True})
            # 兼容旧接口格式
            elif type(event) == type("") or type(event) == type(u""):
                event = event
                if event == "jhddg_every":
                    match["$match"].setdefault("item_count", {"$exists": True})
                else:
                    match["$match"].setdefault("item_count.%s"%event, {"$exists": True})
            elif type(event) == type([]) and len(event) > 1:
                value = event
                tmp = {"$or": []}
                for _key in value:
                    if _key == "jhddg_every":
                        tmp["$or"].append({"item_count": {"$exists": True}})
                    else:
                        tmp["$or"].append({"item_count.%s" % _key: {"$exists": True}})
                match["$match"].setdefault("$and", []).append(tmp)
        query.append(match)
        project = {"$project": {}}
        for event in events[0:i+1]:
            if type(event) == type([]) and len(event) == 1:
                event = event[0]
                if event == "jhddg_every":
                    project["$project"].setdefault("jhd_userkey", 1)
                else:
                    project["$project"].setdefault("jhd_userkey", 1)
                    project["$project"].setdefault("item_count.%s.opatm" % event, 1)
            elif type(event) == type("") or type(event) == type(u""):
                event = event
                if event == "jhddg_every":
                    project["$project"].setdefault("jhd_userkey", 1)
                else:
                    project["$project"].setdefault("jhd_userkey", 1)
                    project["$project"].setdefault("item_count.%s.opatm"%event, 1)
            elif type(event) == type([]) and len(event) > 1:
                value = event
                [project["$project"].setdefault("item_count.%s.opatm"%_key, 1) for _key in value]
        query.append(project)
        group = {"$group": {"_id": "all", "count": {"$sum": 1}, "users": {"$push": "$jhd_userkey"}}}
        query.append(group)
        # print("funnelstepRawData query", json.dumps(query))
        result.setdefault(_tm, {}).setdefault("count", 0)
        result.setdefault(_tm, {}).setdefault("users", set())
        for item in conn[dbname][collection_name].aggregate(query, allowDiskUse=True):
            result.setdefault(_tm, {})["count"] = item["count"]
            result.setdefault(_tm, {})["users"] = item["users"]
    return result


if __name__ == "__main__":
    # http://101.201.145.120:8090/saasapi/eventserisesingle/feeling/2016-09-14/2016-10-13/[%22ac44%22,%22ac17%22,%22ac46%22]/
    # print funnelRawData("biqu", "2016-11-05", "2016-11-13", [["jhddg_every"], ["ac11"], ["ac12"], ["ac13"]])
    # print funnelstep("biqu", "2016-11-13", [[{"id": "ac36", "map": {"og": "SHA"}}], [{"id": "ac9", "map": {}}]])
    a = time.time()
    # print funneldata("biqu", "2016-11-10", "2016-11-20", [[{"id": "ac36", "map": {"og": "SHA"}}], [{"id": "ac9", "map": {}}]])
    # print funneldata("guaeng", "2017-01-01", "2017-01-05", {"funnel": [[{"id": "ac12", "map": {"4": "56d32408c507b600509f48f6"}}], [{"id": "ac6", "map": {"18": "emoji"}}]], "attrs": {"jhd_vr": "2.2.3"}})
    # print funneldata("BIQU_ANDROID", "2016-12-31", "2017-01-07", {"funnel": [[{"id": "jhddg_every"}], [{"id": "ac7"}]], "attrs": {"jhd_vr": "1.1.4"}})
    # print funneldataOld("BIQU_ANDROID", "2016-12-31", "2017-01-07", [["ac1"], ["ac7"]])
    # print funneldata("biqu", "2016-11-10", "2016-11-20", {"funnel": [[{"id": "ac49", "map": {"og": {"$regex": "CSX"}}}], [{"id": "ac23", "map": {}}, {"id": "ac11", "map": {}}], [{"id": "ac22", "map": {}}]]})
    # print funneldata("biqu", "2017-01-10", "2017-01-11", {"attrs": {"jhd_vr": "2.1.4"}, "funnel":[[{"id":"ac9"}],[{"id":"ac36","attrs":[{"id":"og","op":"is","val":"BJS"}]}],[{"id":"ac65","attrs":[{"id":"id","op":"is","val":"123"},{"id":"op","op":"nlike","val":"123"}]},{"id":"ac83","attrs":[{"id":"id","op":"is","val":"想"}]}]]})
    print funneldata("BIQU_ANDROID", "2017-01-04", "2017-01-11", {"funnel":[[{"id":"jhddg_every"}],[{"id":"ac2"}],[{"id":"ac8","attrs":[{"id":"id","op":"like","val":"135"}]}],[{"id":"ac49","attrs":[{"id":"type","op":"nis","val":"经停"},{"id":"wf","op":"is","val":"0"},{"id":"st","op":"endswith","val":"0"}]}],[{"id":"ac50","attrs":[{"id":"name","op":"endswith","val":"亮"},{"id":"hbh","op":"startswith","val":"MU"}]}],[{"id":"ac53","attrs":[{"id":"op","op":"nis","val":"1"}]}],[{"id":"ac55","attrs":[{"id":"type","op":"is","val":"微信"}]}]],"endDay":"2017-01-12","startDay":"2017-01-05"})
    # print funneldata("biqu", "2016-11-10", "2016-11-20", [[{"id": "ac13", "map": {"10": "4"}}], [{"id": "ac44", "map": {}}]])
    print(time.time()-a)
    # print eventsSingle("feeling", "2016-09-14", "2016-10-13", ["ac44", "ac17", "ac46"])
    # print eventsSingle("feeling", "2016-09-01", "2016-09-01", ["in", {"$or": ["ac11"]}, "ac22"])
    # print eventsSingle("feeling", "2016-09-01", "2016-09-01", ["in", "ac11", "ac22"])
    # [["in"], ["ac23", "ac11"], ["ac22"]]