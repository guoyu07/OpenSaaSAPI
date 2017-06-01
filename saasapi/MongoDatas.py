from pymongo import MongoClient
from config import mongo_ip
from config import mongo_port
import json
import itertools
import time
from ipinfo import ipinfo_sina
from SaaSCommon.JHDecorator import fn_timer

from bson.code import Code

global conn
conn = MongoClient(mongo_ip, mongo_port)

def get_collection_name():
    import uuid
    return "_".join(["tmp_", str(uuid.uuid1()).split("-")[0]])

def execute_query(dbname, collection_name, query, return_keys = {}, sortinfo = {"issort": False}):
    global conn
    if sortinfo["issort"] and "sortkey" in sortinfo:
        for item in conn[dbname][collection_name].find(json.loads(query)).sort(sortinfo["sortkey"]):
            yield item
    else:
        for item in conn[dbname][collection_name].find(json.loads(query)):
            yield item

def query_count(dbname, collection_name, query):
    global conn
    return conn[dbname][collection_name].find(json.loads(query)).count()

def query_events_remain(dbname, collection_name, s_tm_str, tar_tm_str_s, tar_tm_str_e, s_event, tar_event):
    global conn
    query_format = '''[{"$match": {"$or": [{"tm": "%(s_tm)s", "item_count.%(s_event)s": {"$exists": true}}, \
    {"tm": {"$gte": "%(tar_tm_str_s)s", "$lte": "%(tar_tm_str_e)s"}, "item_count.%(tar_event)s": {"$exists": true}}]}},\
    {"$group": {"_id": "$jhd_userkey", "groupnum": {"$sum": 1}}}, \
    {"$match": {"groupnum": {"$gt": 1}}}, \
    {"$group": {"_id": "all", "total": {"$sum": 1}}}]'''
    query_str = query_format % {'s_tm': s_tm_str, 'tar_tm_str_s': tar_tm_str_s, 'tar_tm_str_e': tar_tm_str_e, \
                                's_event': s_event, 'tar_event': tar_event}
    query = json.loads(query_str)
    cur = conn[dbname][collection_name].aggregate(query)
    return list(cur)[0]["total"]

def query_events_remain_last(dbname, collection_name, s_tm_str, tar_tm_str_s, tar_tm_str_e, s_event, tar_event):
    global conn
    query_format = '''{"$or": [{"tm": "%(s_tm)s", "item_count.%(s_event)s": {"$exists": true}}, \
    {"tm": {"$gte": "%(tar_tm_str_s)s", "$lte": "%(tar_tm_str_e)s"}, "item_count.%(tar_event)s": {"$exists": true}}]}'''
    query_str = query_format % {'s_tm': s_tm_str, 'tar_tm_str_s': tar_tm_str_s, \
                                'tar_tm_str_e': tar_tm_str_e, 's_event': s_event, 'tar_event': tar_event}
    col_name = get_collection_name()
    query = json.loads(query_str)
    mapper = Code('''
    function(){emit(this.jhd_userkey, {tms: new Array(this.tm)})}
    ''')
    reducer = Code(r'''
    function(key, values){
    var result = {tms: new Array()}
    values.forEach(function(item){item['tms'].forEach(function(item){result['tms'].push(item)})});
    return result;
    }
    ''')
    finalizer = Code('''
    function(key, value){
    values = value['tms']
    var result = values.indexOf("%(s_tm_str)s")>=0 && values.length>=2
    if(result){return 1}else{return 0}
    }
    '''%{"s_tm_str": s_tm_str})
    conn[dbname][collection_name].map_reduce(map=mapper, reduce=reducer, finalize=finalizer, out=col_name,
                                                      query=query)
    event_remain_num = conn[dbname][col_name].find({"value": 1}).count()
    conn[dbname][col_name].drop()
    return event_remain_num


def eventsSep(datatype ,s_tm, e_tm, events = []):
    query_format = '''{"tm": {"$gte": "%(s_tm)s", "$lte": "%(e_tm)s"}, %(find_events)s}'''
    format_events = '''"item_count.%(event)s": {"$exists": true}'''
    find_events_str_lis = []
    for item in events:
        find_events_str_lis.append(format_events % {"event": item})
    query = query_format % {"find_events": ",".join(find_events_str_lis), "s_tm": s_tm, "e_tm": e_tm}
    dbname = datatype
    collection_name = "uvfile"
    return query_count(dbname, collection_name, query)

def query_count_order(dbname, collection_name, query, events, return_keys = {}, sortinfo = {"issort": False}, singlemode = False):
    result = 0
    userkey_tag = None
    event_tms = {}
    for item in execute_query(dbname, collection_name, query, return_keys, sortinfo = {"issort": True, "sortkey": [("jhd_userkey", 1), ("tm", 1)]}):
        userkey = item["jhd_userkey"]
        tm = item["tm"]
        if userkey_tag != userkey and userkey_tag != None:
            eventtm_group = [event_tms[events[i]] for i in range(0, len(events))]
            isseq = is_seqence_events(eventtm_group, singlemode)
            if isseq:
                result += 1
            event_tms = {}
        for event in events:
            event_tms.setdefault(event, [])
            eventinfo = ["+".join([tm, ite]) for ite in item["item_count"][event]["opatm"]]
            event_tms[event] = list(itertools.chain(event_tms[event], eventinfo))
        userkey_tag = userkey
    return result

def is_seqence_events(eventtm_group, singlemode = False):
    start_tm = min(eventtm_group[0])
    accend_tm = start_tm
    exclude_tm = []
    for events in eventtm_group[1:]:
        accend_tm = is_pair_events(accend_tm, events, exclude_tm)
        if accend_tm == None:
            return False
        exclude_tm.append(accend_tm.split("+")[0])
    return True

def is_pair_events(fixed_tm, tms, exclude_tm = [], singlemode = False):
    tms.sort(reverse=False)
    tm = fixed_tm.split("+")[0]
    exclude_tm.append(tm)
    for item in itertools.dropwhile(lambda _tm: _tm <= fixed_tm, tms):
        if singlemode and item.split("+")[0] in exclude_tm:
            continue
        return item
    return None

def is_pair_seq_events(event_optms_a, event_optms_b):
    event_min_a = min(event_optms_a)
    event_max_b = max(event_optms_b)
    return event_min_a <= event_max_b

def eventsCombine(datatype ,s_tm, e_tm, events = [], singlemode = False):
    query_format = '''{"tm": {"$gte": "%(s_tm)s", "$lte": "%(e_tm)s"}, %(find_events)s}'''
    format_events = '''"item_count.%(event)s": {"$exists": true}'''
    find_events_str_lis = []
    for item in events:
        find_events_str_lis.append(format_events % {"event": item})
    query = query_format % {"find_events": ",".join(find_events_str_lis), "s_tm": s_tm, "e_tm": e_tm}

    dbname = datatype
    collection_name = "uvfile"
    return query_count_order(dbname, collection_name, query, events, singlemode = singlemode)

def eventsSeries(datatype, s_tm, e_tm, events = [], singlemode = False):
    result = []
    result.append(eventsSep(datatype ,s_tm, e_tm, events[:1]))
    for i in range(0, len(events)-1):
        num = eventsCombine(datatype, s_tm, e_tm, events[0: i+2])
        result.append(num)
    result_tmp = [{item[0]: item[1]} for item in zip(events, result)]
    return result_tmp

def eventsSingle(datatype, s_tm_str, e_tm_str, events = []):
    s_stamp = time.mktime(time.strptime(s_tm_str, "%Y-%m-%d"))
    e_stamp = time.mktime(time.strptime(e_tm_str, "%Y-%m-%d"))
    result = {}
    while s_stamp <= e_stamp:
        tm = time.strftime("%Y-%m-%d", time.localtime(s_stamp))
        result_item = eventsSeries(datatype, tm, tm, events, singlemode = True)
        result.setdefault(tm, result_item)
        s_stamp += 86400
    return _sortByKey(result)

def eventsRemain(datatype, s_tm_str, lasttm, events = []):
    s_stamp = time.mktime(time.strptime(s_tm_str, "%Y-%m-%d"))
    result = {}
    collection_name = "uvfile"
    s_event = events[0]
    tar_event = events[1]
    result.setdefault(s_tm_str, query_count(datatype, collection_name, '''{"tm": "%(tm)s", "item_count.%(s_event)s": {"$exists": true}}''' % {"tm": s_tm_str, "s_event": s_event}))
    for i in range(1, lasttm+1):
        if s_stamp+86400*i > time.time()-86400:
            break
        tm = time.strftime("%Y-%m-%d", time.localtime(s_stamp+86400*i))
        result_item = query_events_remain(datatype, collection_name, s_tm_str, tm, tm, s_event, tar_event)
        result.setdefault(tm, result_item)
    return _sortByKey(result)

def eventsRemain_combine(datatype, tm_str_s, tm_str_e, lasttm, events = []):
    s_stamp = time.mktime(time.strptime(tm_str_s, "%Y-%m-%d"))
    e_stamp = time.mktime(time.strptime(tm_str_e, "%Y-%m-%d"))
    result = {}
    collection_name = "uvfile"
    s_event = events[0]
    tar_event = events[1]
    while s_stamp <= e_stamp:
        tm_str_s = time.strftime('%Y-%m-%d', time.localtime(s_stamp))
        count_1 = query_count(datatype, collection_name, '''{"tm": "%(tm)s", "item_count.%(s_event)s": {"$exists": true}}''' % {"tm": tm_str_s, "s_event": s_event})
        tm_s = time.strftime('%Y-%m-%d', time.localtime(s_stamp+86400*1))
        tm_e = time.strftime('%Y-%m-%d', time.localtime(s_stamp+86400*lasttm))
        count_2 = query_events_remain_last(datatype, collection_name, tm_str_s, tm_s, tm_e, s_event, tar_event)
        result.setdefault(tm_str_s, [count_1, count_2])
        s_stamp += 86400
    return _sortByKey(result)

def _sortByKey(data, reverse = True):
    import collections
    _keys = data.keys()
    _keys.sort(reverse=reverse)
    tmp = collections.OrderedDict()
    for key in _keys:
        tmp[key] = data[key]
    return tmp

def userSample(datatype, tm, num = 200):
    global conn
    collection_name = "uvfile"
    import random
    seednum_1 = random.randint(1, 1000)
    seednum_2 = random.randint(1, 1000)
    seednum_min = min([seednum_1, seednum_2])
    seednum_max = max([seednum_1, seednum_2])
    query_format = '''{"tm": "%(tm)s", "random": {"$gte": %(seednum_min)d, "$lte": %(seednum_max)d}, \
    "item_count.in": {"$exists": true}}'''
    query = query_format % {"tm": tm, "seednum_min": seednum_min, "seednum_max": seednum_max}
    result = []
    for item in conn[datatype][collection_name].find(json.loads(query)).limit(num):
        user_opas = userPath(item)
        if user_opas['opas'][0][0] != 'in':
            continue
        result.append(user_opas)
    return result

def userPath(data):
    result = {}
    for key in data:
        if type(data[key]) == type([]):
            result[key] = data[key][0] if len(data[key]) != 0 else ''
        elif type(data[key]) == type(''):
            result[key] = data[key]
        elif key == "item_count":
            item_count = data[key]
            tmp = {}
            for _key in item_count:
                if _key in set(['page', 'action']):
                    continue
                opatype = item_count[_key]["eventtype"]
                [tmp.setdefault((opatype, opatm), _key) for opatm in item_count[_key]["opatm"]]
            tmp_sorted = sorted(tmp.items(), key = lambda item: item[0][1], reverse=False)
            result["opas"] = [[item[0][0], item[0][1], item[1]] for item in tmp_sorted]
    return result

def userSample_lite(datatype, tm, num = 200):
    global conn
    collection_name = "uvfile"
    import random
    seednum_1 = random.randint(1, 1000)
    seednum_2 = random.randint(1, 1000)
    seednum_min = min([seednum_1, seednum_2])
    seednum_max = max([seednum_1, seednum_2])
    query_format = '''{"tm": "%(tm)s", "random": {"$gte": %(seednum_min)d, "$lte": %(seednum_max)d}, \
    "item_count.in": {"$exists": true}}'''
    query = query_format % {"tm": tm, "seednum_min": seednum_min, "seednum_max": seednum_max}
    result = []
    for item in conn[datatype][collection_name].find(json.loads(query)).limit(num):
        user_opas = transform_uv_record(item)
        result.append(user_opas)
        if len(result) >= 30:
            break
    return result

def transform_uv_record(data):
    result = {}
    for key in data:
        if type(data[key]) == type([]):
            if key == 'jhd_ip':
                loc = ipinfo_sina(data[key][0]) if len(data[key]) != 0 else ('', '', '')
                result["loc"] = [loc[0], loc[1], loc[2]]
            result[key] = data[key][0] if len(data[key]) != 0 else ''
        elif type(data[key]) == type('') or type(data[key]) == type(unicode('')):
            result[key] = data[key]
        elif key == "item_count":
            item_count = data[key]
            actions = set()
            for _key in item_count:
                if _key in set(['action', 'page']):
                    continue
                opatype = item_count[_key]["eventtype"]
                if opatype != 'action':
                    continue
                actions.add(_key)
            result["actions"] = list(actions)
        elif key == "item_add":
            dur = data[key].get('end', 0.0)
            result["dur"] = dur
    return result

def search_user(datatype, dayStr, hour_s, hour_e, base_cond):
    global conn
    assert type(base_cond) == type({}), "type base_cond error"
    hours = map(lambda item: str(item) if item >=10 else "".join(["0", str(item)]), \
                [item for item in range(hour_s, hour_e+1)])
    hour_format = '''{"item_count.action.opatm": {"$regex": "^%s.*"}}'''
    hour_cond = json.loads("".join(["[", ", ".join(map(lambda hour: hour_format % hour, hours)), "]"]))
    base_cond["$or"] = hour_cond
    base_cond["tm"] = dayStr
    result = []
    for item in conn[datatype]["uvfile"].find(base_cond).limit(100):
        user_opas = transform_uv_record(item)
        result.append(user_opas)
    return result

if __name__ ==  "__main__":
    print eventsSingle('feeling', '2016-07-25', '2016-07-26', ["in","ac23","end"])
    # iter = execute_query('feeling', 'uvfile', '''{"tm": "2016-07-05", "$or": [{"item_count.action.opatm": {"$regex": "^10.*"}}, {"item_count.action.opatm": {"$regex": "^11.*"}}]}''')
    # print len(list(iter))
    # print search_user('feeling', '2016-07-05', 1, 11, {})
    # print get_collection_name()
    # print eventsRemain('feeling', '2016-06-16', 7, ['in', 'ac23'])
    # line = {u'jhd_ua': [u'iphone7_1'], u'jhd_pb': [u'appstore'], u'jhd_netType': [u'wifi'], u'jhd_vr': [u'2.0.1'], u'jhd_userkey': u'b4e48c13-12da-460b-bb49-ada7c348f8d6', u'random': 215, u'jhd_os': [u'iphone_9.3.1'], u'item_count': {u'ms_5708447039b0570053b9fd6f': {u'eventtype': u'page', u'opatm': [u'21:22:44', u'21:22:54'], u'num': 2}, u'ac14': {u'eventtype': u'action', u'opatm': [u'21:22:39', u'21:22:51', u'21:23:14', u'21:23:23'], u'num': 4}, u'end': {u'eventtype': u'end', u'opatm': [u'21:21:32', u'21:23:27'], u'num': 2}, u'ms_55970c0be4b04fe7024e2e74': {u'eventtype': u'page', u'opatm': [u'21:23:21'], u'num': 1}, u'ml': {u'eventtype': u'page', u'opatm': [u'21:20:17'], u'num': 1}, u'ms_576e807c5bbb5000595b84e5': {u'eventtype': u'page', u'opatm': [u'21:20:24', u'21:22:42'], u'num': 2}, u'ac44': {u'eventtype': u'action', u'opatm': [u'21:20:23'], u'num': 1}, u'page': {u'eventtype': u'page', u'opatm': [u'21:20:17', u'21:20:17', u'21:20:20', u'21:20:24', u'21:22:42', u'21:22:44', u'21:22:54', u'21:23:10', u'21:23:16', u'21:23:21'], u'num': 10}, u'msl': {u'eventtype': u'page', u'opatm': [u'21:20:20'], u'num': 1}, u'ms_56bbef14816dfa3544bf0366': {u'eventtype': u'page', u'opatm': [u'21:23:10', u'21:23:16'], u'num': 2}, u'in': {u'eventtype': u'in', u'opatm': [u'21:22:37'], u'num': 1}, u'action': {u'eventtype': u'action', u'opatm': [u'21:20:23', u'21:22:39', u'21:22:51', u'21:23:14', u'21:23:23'], u'num': 5}, u'nu': {u'eventtype': u'page', u'opatm': [u'21:20:17'], u'num': 1}}, u'jhd_pushid': u'571f6779df0eea0062b7e8d0', u'jhd_datatype': u'feeling', u'tm': u'2016-07-03', u'item_add': {u'end': 124.626064, u'in': 65.361366}, u'_id': '5779b55ffb6b6d378d4da1d3', u'jhd_ip': [u'183.128.212.66']}
    # print transform_uv_record(line)
    # result = userSample_lite('feeling', '2016-07-03')
    # for item in result:
    #     print item
    # print query_events_remain('feeling', 'uvfile', '2016-06-16', '2016-06-18', 'in', 'ac23')
    # print is_pair_events("2016-06-17000005", ["2016-06-17000001", "2016-06-17000001", "2016-06-17000006"])
    # print eventsSep("feeling", "2016-06-17", "2016-06-17", ['dl', 'ac23'])
    # print eventsSeries("feeling", "2016-06-16", "2016-06-17", ['dl', 'ac23'])
    # return_keys = {}
    # dbname = "feeling"
    # collection_name = "uvfile"
    # query = '''{"tm": {"$gte": "2016-06-17", "$lte": "2016-06-17"}, "item_count.dl": {"$exists": true}}'''
    # print conn[dbname][collection_name].find(json.loads(query), json.loads(return_keys)).count()
