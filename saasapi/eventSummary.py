# -*- coding: utf-8 -*-
from pymongo import MongoClient
from config import mongo_ip
from config import mongo_port
import json
import time
from SaaSCommon.JHDecorator import fn_timer

global conn
conn = MongoClient(mongo_ip, mongo_port)

re_char = [".", "+", "?", "*", "$"]


@fn_timer
def eventSummary(datatype, querydata={}, mode = "UserEventGroup"):
    # 添加模糊匹配 url_pattern
    if datatype == "BQ_H5" and querydata.get("jhd_opType", None) == "page":
        table = "UserEvent"
    else:
        table = "UserEventGroup"
    global conn
    tm_s = querydata["startDay"].replace("-", "")
    tm_e = querydata["endDay"].replace("-", "")
    tm_s_stamp = time.mktime(time.strptime(tm_s, "%Y%m%d"))
    tm_e_stamp = time.mktime(time.strptime(tm_e, "%Y%m%d"))
    num = (tm_e_stamp - tm_s_stamp)/86400
    # print("="*20, tm_s, tm_e, num)
    # 2016-12-15日共同商定时间跨度65天
    if num > 65 or num < 0:
        return ["Out of date range"]
    optype = querydata.get("jhd_opType", "action")
    if tm_s < '20161001':
        return []
    events = querydata.get("events", [])
    url_pattern = querydata.get("url_pattern", None)
    pub = querydata.get("jhd_pb", "")
    vr = querydata.get("jhd_vr", "")

    match = '''{"$match": {"partition_date": {"$gte": "%(tm_s)s", "$lte": "%(tm_e)s"}, "jhd_opType": "%(optype)s"}}''' % {
        "tm_s": tm_s,
        "tm_e": tm_e,
        "optype": optype
    }
    match = json.loads(match)
    if events:
        if datatype == "BQ_H5" and querydata.get("jhd_opType", None) == "page":
            match["$match"]["jhd_map.uri"] = {"$in": events}
        else:
            match["$match"]["jhd_eventId"] = {"$in": events}
    if url_pattern:
        if url_pattern:
            for re_c in re_char:
                url_pattern = url_pattern.strip().replace(re_c, "\\"+re_c)
            match["$match"]["jhd_map.uri"] = {"$regex": url_pattern}
    if pub and pub != "all":
        match["$match"]["jhd_pb"] = pub
    if vr and vr != "all":
        match["$match"]["jhd_vr"] = vr
    match = json.dumps(match)
    if table == "UserEventGroup":
        mongoquery = '''[
        %(match)s,
        {"$project": {"jhd_userkey": 1, "jhd_eventId": 1}},
        {"$group": {"_id": {"eventid": "$jhd_eventId", "uid": "$jhd_userkey"}}},
        {"$group": {"_id": "$_id.eventid", "uv": {"$sum": 1}}}
        ]''' % {"match": match}
    elif table == "UserEvent":
        '''
        %(match)s,
        {"$project": {"jhd_userkey": 1, "uri": "$jhd_map.uri"}},
        {"$group": {"_id": "$uri", "uids": {"$addToSet": "$jhd_userkey"} }},
        {"$project": {"_id": "$_id", "uv": {"$size": "$uids"}}}
        ]
        '''
        mongoquery = '''[
        %(match)s,
        {"$project": {"jhd_userkey": 1, "uri": "$jhd_map.uri"}},
        {"$group": {"_id": "$uri", "uids": {"$addToSet": "$jhd_userkey"} }},
        {"$project": {"_id": "$_id", "uv": {"$size": "$uids"}}},
        {"$match": {"uv": {"$gte": 2}}}
        ]''' % {"match": match} # {"$sort": {"uv": -1}}
    # print(table, )
    print(mongoquery)
    query_result_cur = conn[datatype][table].aggregate(json.loads(mongoquery), allowDiskUse=True)
    result = [item for item in query_result_cur]
    return result


if __name__ == "__main__":
    # {"startDay": "2016-10-28", "endDay": "2016-11-20", "events": ["ac10"],  " jhd_opType ": "action"}
    # print json.dumps(eventSummary("BQ_H5", querydata={"startDay": "2016-10-28", "endDay": "2016-11-20", "events": ["ac10"]}))

    # print json.dumps(eventSummary("BQ_H5", querydata={"startDay": "2016-12-01", "endDay": "2016-12-13", "jhd_opType": "page"}, mode = "UserEvent"))
    print json.dumps(eventSummary("BQ_H5", querydata={"endDay":"2016-12-15","startDay":"2016-10-11","jhd_opType":"page"}, mode = "UserEvent"))
    # print json.dumps(eventSummary("BQ_H5", querydata={"endDay":"2016-12-13","startDay":"2016-11-14","jhd_opType":"page","events":["https://m.biqu.panatrip.cn/app?openid=oqfl0uLI_EnV8gOowE-hyBYeFV28"]}, mode = "UserEvent"))