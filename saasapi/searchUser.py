# coding: utf-8
from pymongo import MongoClient
from config import mongo_ip
from config import mongo_port
import json
import copy
import time
import itertools
# from ipinfo import ipinfo_sina
from IPtoLoc.iploc_demo import getLoc
from apple_jx import apple_jx
from SaaSCommon.JHDecorator import fn_timer

from DBClient.MysqlClient import MysqlClient
from DBClient.PyMongoClient import PyMongoClient


def get_mongo_conn(appkey):
    m_client = MysqlClient()
    mongo_id = m_client.get_mongoid(appkey)[0]
    m_client.closeMysql()
    conn = PyMongoClient(mongo_id=mongo_id)
    return conn.getConn()


# data format
# example
# {
#     "session": {
#         "action": [
#             "ac6"
#         ],
#         "seconds": 46002,
#         "sessionsbegintm": "2016-07-21+23:59:59",
#         "sessionendtm": "2016-07-21+23:59:59"
#     },
#     "jhd_pb": [
#         "appstore"
#     ],
#     "jhd_netType": [
#         "wifi"
#     ],
#     "lastActiveInterval": 0,
#     "loc": [
#         "中国",
#         "广东",
#         "广州"
#     ],
#     "jhd_userkey": "0a723853-1de2-4d37-8eee-faa1e0cedc58",
#     "jhd_vr": "2.0.1",
#     "jhd_os": [
#         "iphone_9.3.2"
#     ],
#     "jhd_pushid": "56d97a3a816dfa005a38b738",
#     "jhd_datatype": "feeling",
#     "jhd_ua": "iPhone 6 Plus",
#     "last7ActiveNum": 4,
#     "jhd_ip": [
#         "116.22.44.81"
#     ],
#     "lastOpaTime": "2016-07-21+12:13:45",
#     "last30ActiveNum": 8
# }

singlekey = ["jhd_datatype", "jhd_userkey", "jhd_pushid", "lastOpaTime"]
appendkey = ["jhd_pb", "jhd_vr", "jhd_os", "jhd_netType", "jhd_ua", "jhd_ip", "jhd_loc"]

global basic_items
basic_items = list(itertools.chain(singlekey, appendkey))
global tm
tm = None

@fn_timer
def search_user(datatype, table='uvfile', _filter={}):
    assert _filter, "None filter"
    global tm
    tm = _filter["tm"]
    print("start _filter", json.dumps(_filter))

    conn = get_mongo_conn(datatype)

    if _filter.get("lastOpaTime", None):
        # 如果开始时间没有设置，则设置为"00:00:00"，如果结束时间没有设置，设置为"23:59:59"
        starttm = ":".join([_filter["lastOpaTime"]["$gte"].split("+")[1], "00:00"]) if _filter["lastOpaTime"].get("$gte", None) else "00:00:00"
        endtm = ":".join([_filter["lastOpaTime"]["$lte"].split("+")[1], "00:00"]) if _filter["lastOpaTime"].get("$lte", None) else "23:59:59"
    else:
        # 如果没有设置lastOpaTime过滤条件
        starttm = "00:00:00"
        tm = _filter.get("tm", time.strftime("%Y%m%d", time.localtime(time.time()))).replace("-", "")
        # 如果日期过滤设置的不是今天，则设置enttm为"23:59:59"
        if tm < time.strftime("%Y%m%d", time.localtime(time.time())):
            endtm = "23:59:59"
        else:
            # 如果过滤日期为当天，设置最后时间为当前时间(过滤昨天的日志，今天才收到的异常时间点事件)
            endtm = time.strftime("%H:%M:%S", time.localtime(time.time()))
    # 处理用户最近来访天数
    measure_keys = ["measure.last7ActiveNum", "measure.last30ActiveNum"]
    for measurekey in measure_keys:
        if _filter.get(measurekey, None):
            boundary_down_key = "$gte" if "$gte" in _filter[measurekey] else "$gt"
            boundary_up_key = "$lte" if "$lte" in _filter[measurekey] else "$lt"
            # boundary_down = _filter[measurekey][boundary_down_key] - 1
            # boundary_up = _filter[measurekey][boundary_up_key] - 1
            boundary_down = _filter[measurekey][boundary_down_key]
            boundary_up = _filter[measurekey][boundary_up_key]
            _filter[measurekey] = {boundary_down_key: boundary_down, boundary_up_key: boundary_up}
    # 用户时间段抽样
    filter_begin_tm = None
    filter_end_tm = None
    if _filter.get("actm", None):
        _filter["item_count.action.opatm"] = _filter["actm"]

        filter_begin_tm = _filter["actm"]["$gte"].replace(":", "") if "$gte" in _filter["actm"] else "000000"
        filter_end_tm = _filter["actm"]["$lte"].replace(":", "") if "$lte" in _filter["actm"] else "235959"
        del _filter["actm"]
    if _filter.get("jhd_pb", None) and type(_filter["jhd_pb"]) != type({}):
        _filter["jhd_pb"] = {"$regex": _filter["jhd_pb"], "$options": "i"}
    result = []

    get_result(conn, datatype, table, _filter, starttm, endtm, filter_begin_tm, filter_end_tm, result)
    if len(result) == 0 and "jhd_userkey" in _filter:
        uid = _filter.pop("jhd_userkey")
        _filter["jhd_pushid"] = uid
        get_result(conn, datatype, table, _filter, starttm, endtm, filter_begin_tm, filter_end_tm, result)

    result_sorted = sorted(result, key=lambda _item: _item["session"]["sessionsbegintm"], reverse = True)
    tmp = _valid_result(result_sorted, _filter)
    _tmp = []
    for item in tmp:
        try:
            json.dumps(item, ensure_ascii=False)
            _tmp.append(item)
        except:
            import traceback
            print(traceback.print_exc())
    return _tmp

def get_result(conn, datatype, table, _filter, starttm, endtm, filter_begin_tm, filter_end_tm, result):
    for item in conn[datatype][table].find(_filter).limit(500):
        try:
            userkey = item["jhd_userkey"]
            if (not userkey) or len(userkey) < 10:
                continue
            if userkey in ["-1", "", "0", "00000000-0000-0000-0000-000000000000", "unknown", "739463", "000000000000000","111111111111111","352005048247251","012345678912345", "012345678901237", "88508850885050", "0123456789abcde","004999010640000", "862280010599525", "52443443484950", "355195000000017", "001068000000006", "358673013795895", "355692547693084", "004400152020000", "8552502717594321","113456798945455", "012379000772883", "111111111111119", "358701042909755", "358000043654134", "345630000000115", "356299046587760", "356591000000222","9774d56d682e549c"]:
                continue
            # item
            _sort_data(item)
            _item = _split_session(item, starttm, endtm)
            # 根据时间段筛选会话
            _item_tmp = []
            if filter_begin_tm and filter_end_tm:
                for item in _item:
                    sessionsbegintm = item["session"]["sessionsbegintm"].replace("-", "").replace("+", "").replace(":", "")
                    sessionendtm = item["session"]["sessionendtm"].replace("-", "").replace("+", "").replace(":", "")
                    if (sessionsbegintm[-6:] >= filter_begin_tm and sessionsbegintm[-6:] <= filter_end_tm) or \
                            (sessionendtm[-6:] >= filter_begin_tm and sessionendtm[-6:] <= filter_end_tm):
                        _item_tmp.append(item)
                tmp = _item_tmp if ("jhd_userkey" in _filter or "jhd_pushid" in _filter) else _item_tmp[-4:]
                result += tmp
            else:
                tmp = _item if ("jhd_userkey" in _filter or "jhd_pushid" in _filter) else _item[-4:]
                result += tmp
        except:
            import traceback
            print(traceback.print_exc())

def _sort_data(data):
    if data.get("item_count", None):
        item_count = data["item_count"]
        for key in item_count.keys():
            # 事件序列排序
            item_count[key]["opatm"].sort()

def _find_dic_key(data, _like):
    for key in data:
        if _like in key:
            yield key

def _valid_result(result_sorted, _filter):
    tmp = []
    # print(result_sorted, len(result_sorted), "result_sorted")
    for item in result_sorted:
        try:
            if "jhd_vr" in _filter and type(_filter.get("jhd_vr", {})) == type({}):
                vr = _filter.get("jhd_vr", {}).get("$regex", "")
                vrs = [_vr for _vr in item["jhd_vr"] if vr in _vr]
                item["jhd_vr"] = vrs[0] if len(vrs) else ""
            elif "jhd_vr" in _filter and type(_filter.get("jhd_vr", "")) == type(""):
                item["jhd_vr"] = _filter["jhd_vr"]
            else:
                item["jhd_vr"] = item.get("jhd_vr", [""])[-1]

            if "jhd_userkey" not in _filter and "jhd_pushid" not in _filter:
                if not item.get("session", {"seconds": 0}).get("seconds", 0):
                    continue
                if not item.get("session", {"action": []}).get("action", []):
                    continue
            else:
                item.setdefault("session", {"action": []})
            session_keys = item.get("session", {}).keys()
            if ("action" not in session_keys and "page" not in session_keys and "in" not in session_keys):
                # print('''"action" not in session_keys and "page" not in session_keys and "in" not in session_keys''', "continue", session_keys)
                continue
            actions = [key.split(".")[1].split("_")[1] if key.split(".")[1].startswith("jhf_") else key.split(".")[1] for key in _find_dic_key(_filter, "item_count")]
            # 如果按照访问时间筛选，则加入action“动作”
            if len(actions) == 1 and "action" in actions:
                del actions[actions.index("action")]
            # actions = [key.split(".")[1] for key in _find_dic_key(_filter, "item_count")]
            if not all([(action in item["session"].get("action", [])) for action in actions]):
                continue
            # item["loc"] = ipinfo_sina(item.get("jhd_ip", [""])[0])
            try:
                loc = item.get("jhd_loc", [None])[0]
            except:
                loc = None
            try:
                del item["jhd_loc"]
            except:
                pass
            if loc:
                if isinstance(loc, dict):
                    country, prov, city = "", loc.get("prov", ""), loc.get("city", "")
                else:
                    try:
                        country, prov, city = "unknown", loc.split("_")[0], loc.split("_")[1]
                    except:
                        country, prov, city = "unknown", "", ""
                item["loc"] = (country, prov, city)
            else:
                item["loc"] = getLoc(item.get("jhd_ip", [""])[0])
            if u"中国" == item["loc"][2] and ("jhd_userkey" not in _filter and "jhd_pushid" not in _filter):
                continue
            if item["jhd_ua"]:
                item["jhd_ua"] = apple_jx.get(item["jhd_ua"][0], item["jhd_ua"][0])
            tmp.append(item)
            if "jhd_userkey" not in _filter and "jhd_pushid" not in _filter:
                if len(tmp) >= 500:
                    break
        except:
            import traceback
            print(traceback.print_exc())
    return tmp

def _split_session(data, starttm = '00:00:00', endtm = '23:59:59'):
    """
    # 用户session分割
    :param data: 带分割数据
    :param starttm: 时间段过滤开始时间
    :param endtm: 时间段过滤结束时间
    :return: 分割的session集合
    """
    global basic_items
    global tm
    result = []
    # 用户基础属性
    sessoin_result = {}
    tm = data['tm']
    for key in data:
        # basic_items(uvfile 用户基本属性)
        if key in basic_items:
            try:
                # 如果key不是list类型直接赋值，如果为list类型则取最后一个值
                sessoin_result.setdefault(key, data[key]) \
                    if ((key not in set(["jhd_vr"]) and type(data[key])) != type([])) \
                    else sessoin_result.setdefault(key, data[key][-1:])
            except:
                import traceback
                print(traceback.print_exc())
        elif key == "measure":
            # 用户访问属性值设置
            sessoin_result.setdefault("last7ActiveNum", data[key].get("last7ActiveNum", 1))
            sessoin_result.setdefault("last30ActiveNum", data[key].get("last30ActiveNum", 1))
            sessoin_result.setdefault("lastActiveInterval", data[key].get("lastActiveInterval", 1))
        elif key == 'item_count':
            # 根据item_count生成(opatime, eventtype, event)元组对
            # tmp = [("".join([ct, "0"]) if data[key][_key]["eventtype"] != "end" else "".join([ct, "1"]), data[key][_key]["eventtype"], _key) \
            #        for _key in data[key] for ct in data[key][_key]["opatm"] \
            #        # 去掉action，page汇总数据
            #        if _key not in ['action', 'page'] and (ct >= starttm and ct <= endtm)]
            tmp = []
            # 稳定排序转化，操作时间相同时，in尽量往前放，end尽量往后放
            for _key in data[key]:
                # 排除两个in间隔30秒之内的情况
                in_pre = time.mktime(time.strptime('19880101000000', '%Y%m%d%H%M%S'))
                end_pre = time.mktime(time.strptime('19880101000000', '%Y%m%d%H%M%S'))
                for ct in data[key][_key]["opatm"]:
                    # if key == "in":
                    if _key == "in":
                        dayStr = (tm + ct).replace("-", "").replace(":", "")
                        in_cur = time.mktime(time.strptime(dayStr, '%Y%m%d%H%M%S'))
                        if in_cur - in_pre < 30:
                            in_pre = in_cur
                            continue
                        else:
                            in_pre = in_cur
                    # elif key == "end":
                    elif _key == "end":
                        dayStr = (tm + ct).replace("-", "").replace(":", "")
                        end_cur = time.mktime(time.strptime(dayStr, '%Y%m%d%H%M%S'))
                        if end_cur - end_pre < 30:
                            end_pre = end_cur
                            continue
                        else:
                            end_pre = end_cur
                    if _key in ['action', 'page', 'jhf_dur'] and (ct >= starttm and ct <= endtm):
                        continue
                    elif _key.startswith("jhf_"): # 排除自定义 action
                        continue
                    if data[key][_key]["eventtype"] == "in":
                        tmp.append(("".join([ct, "0"]), data[key][_key]["eventtype"], _key))
                    elif data[key][_key]["eventtype"] == "end":
                        tmp.append(("".join([ct, "6"]), data[key][_key]["eventtype"], _key))
                    else:
                        tmp.append(("".join([ct, "5"]), data[key][_key]["eventtype"], _key))

            tmp = list(set(tmp))
            # 按opatime时间排序
            opa_sort = sorted(tmp, key=lambda item: item[0], reverse=False)
            opa_sort = [(item[0][:-1], item[1], item[2]) for item in opa_sort]
    if not opa_sort:
        return []
    pre = 0 # 上一次时间
    ct_pre = None  # 上次时间（字符型）

    sessoin_opas = {}
    item = None
    # eventid = None
    for item in opa_sort:
        try:
            ct, eventtype, eventid = item
            if eventid.startswith("jhf"):
                continue
            # print ct, eventtype, eventid
            # datetime 当天时间
            cur = time.mktime(time.strptime("".join([tm.replace("-", ""), ct.replace(":", "")]), "%Y%m%d%H%M%S"))
            # if pre == 0 and eventtype == "end":
            #     continue
            if pre == 0:
                sessoin_opas["seconds"] = sessoin_opas.setdefault("seconds", 0.0)
                ct_pre = ct
            else:
                if cur-pre <= 600:
                    if sessoin_opas:
                        sessoin_opas["seconds"] = sessoin_opas.setdefault("seconds", 0.0) + (cur - pre)
                    else:
                        sessoin_opas["seconds"] = sessoin_opas.setdefault("seconds", 0.0)
                else:
                    sessoin_opas["seconds"] = sessoin_opas.setdefault("seconds", 0.0)
            sessoin_opas.setdefault("sessionsbegintm", "+".join([tm, ct]))  # 设置session开始时间
            # 如果(间隔大于600)(或者事件为end)，向前切分会话,并排除操作为 in 的情况
            if ((cur-pre > 600 and pre != 0) or eventtype == "end") and eventtype != "in":
                # 获取用户基础属性
                basic_data = copy.deepcopy(sessoin_result)
                # 如果为新session产生，则把当前时间加入session_opas对应的eventtype中
                if eventtype == "end":
                    sessoin_opas.setdefault(eventtype, []).append(eventid) \
                        if eventid not in sessoin_opas.get(eventtype, []) else sessoin_opas.setdefault(eventtype, [])
                if cur-pre <= 600 and sessoin_opas != {}:
                    sessoin_opas.setdefault("sessionendtm", "+".join([tm, ct]))
                else:
                    sessoin_opas.setdefault("sessionendtm", "+".join([tm, ct_pre]))
                basic_data.setdefault("session", sessoin_opas)
                if sessoin_opas.get("sessionendtm", "2999-12-31+23:59:59") <= time.strftime("%Y-%m-%d+%H:%M:%S", time.localtime(time.time())):
                    result.append(basic_data)
                sessoin_opas = {}
            # 当操作类型为in时，向后切分会话
            elif eventtype == "in":
                if pre == 0:
                    ct_pre = ct
                basic_data = copy.deepcopy(sessoin_result)
                sessoin_opas.setdefault("sessionendtm", "+".join([tm, ct_pre]))
                basic_data.setdefault("session", sessoin_opas)
                if "+".join([tm, ct_pre]) <= time.strftime("%Y-%m-%d+%H:%M:%S", time.localtime(time.time())):
                    result.append(basic_data)
                sessoin_opas = {}
                sessoin_opas.setdefault("sessionsbegintm", "+".join([tm, ct]))  # 设置session开始时间
                sessoin_opas.setdefault(eventtype, []).append(eventid) \
                    if eventid not in sessoin_opas.get(eventtype, []) else sessoin_opas.setdefault(eventtype, [])
            if eventtype != "end":
                # 按先后顺序归类事件
                sessoin_opas.setdefault(eventtype, []).append(eventid) \
                    if eventid not in sessoin_opas.get(eventtype, []) else sessoin_opas.setdefault(eventtype, [])
            pre = cur
            ct_pre = ct
        except:
            import traceback
            print(traceback.print_exc(), item)
    ct, eventtype, eventid = item
    sessoin_opas.setdefault(eventtype, []).append(eventid) \
        if eventid not in sessoin_opas.get(eventtype, []) else sessoin_opas.setdefault(eventtype, [])
    basic_data = copy.deepcopy(sessoin_result)
    sessoin_opas.setdefault("sessionendtm", "+".join([tm, ct_pre]))
    basic_data.setdefault("session", sessoin_opas)
    if "sessionsbegintm" in basic_data.get("session", {}) and "+".join([tm, ct_pre]) <= time.strftime("%Y-%m-%d+%H:%M:%S", time.localtime(time.time())):
        result.append(basic_data)
    return result


if __name__ == "__main__":

    data = '''{"tm": "2016-07-21", "jhd_vr": "2.0.1", "jhd_ua": {"$regex": "iphone"}, "item_count.ac23": {"$exists": true}, "firstLoginTime":  {"$regex": "20160721"}, "lastOpaTime": {"$gte": "2016-07-21+09", "$lte": "2016-07-21+17"}}'''
    data = '''{"tm":"2016-10-08", "actm": {"$gte": "00:00:00", "$lte": "17:00:00"}}'''
    data = '''{"tm":"2016-10-08", "jhd_userkey": "434A4B68-B48C-4CB9-809E-54FDF25B1DA9", "item_count.ac34": {"$exists": true}}'''
    data = '''{"tm":"2016-10-10", "item_count.ac36": {"$exists": true}}'''
    data = '''{"tm":"2016-12-28","actm":{"$gte":"02:02:00"},"measure.firstLoginTime":{"$regex":"20161208"}}'''
    # data = '''{"tm":"2016-10-08", "item_count.ac34": {"$exists": true}}'''
    # data = '''{"tm":"2016-10-08"}'''
    data = '''{"tm":"2017-03-10"}'''
    _filter = json.loads(data)
    a = time.time()
    data = search_user("jinjiedao", "uvfile", _filter)
    print(time.time()-a)
    print(json.dumps(data, ensure_ascii=False))
    print(len(data))

