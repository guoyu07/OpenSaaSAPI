# coding: utf-8
import __init__
from django.http import HttpResponse

from funnel import funneldataOld
from funnel import funneldata
from eventRemain import eventsRemain
from eventRemainMap import eventsRemainMap
from eventRemain import eventsRemainCombine
from crossEvent import eventsCrossCombine
from crossEventMap import crossEventMap
from searchUser import search_user as rt_search
from eventSummary import eventSummary

from MongoDatas import userSample
from MongoDatas import userSample_lite
from MongoDatas import search_user
from MongoDatas import eventsSeries

import urllib
import json

from jhddgapi.settings import DEBUG


# global
# from IPtoLoc.__init__ import ipdataPath
# from IPtoLoc import IPtoAreaFinals
# global initarry
# if initarry is None:
#     initarry = IPtoAreaFinals.load(ipdataPath)

# Create your views here.


def getEventsSeries(request, datatype, s_tm, e_tm, events_quote):
    # ['dl', 'ac23']
    try:
        events_str = urllib.unquote(events_quote)
        events = json.loads(events_str)
        result = eventsSeries(datatype, s_tm, e_tm, events)
        # data = json.dumps(result, ensure_ascii=False)
        data = json.dumps(result).decode('unicode-escape').encode('utf8')
        return HttpResponse(data)
    except:
        import traceback
        exstr = traceback.format_exc()
        if DEBUG:
            print(exstr)
            return HttpResponse(exstr)
        else:
            print(exstr)


# 事件漏斗,线上
def getFunnel(request, datatype, params):
    '''
    :param request:
    :param datatype:
    :param s_tm:
    :param e_tm:
    :param events_quote:
    :format url: http://101.201.145.120:8090/saasapi/funnel/biqu/{"startDay": "2016-11-10", "endDay": "2016-11-20", "funnel": [[{"id": "jhddg_every", "map": {}}], [{"id": "ac41", "map": {"og": "CSX"}}], [{"id": "ac23", "map": {}}, {"id": "ac11", "map": {}}], [{"id": "ac22", "map": {}}]]}/
    :return:
    '''
    try:
        params = urllib.unquote(params)
        data = json.loads(params)
        startDay = data["startDay"]
        endDay = data["endDay"]
        # if datatype not in ["biqu", "BIQU_ANDROID", "biqu_all", "feeling", "ncf_h5", "guaeng", "caiyu_ad"]:
        if False:
            result = funneldata(datatype, startDay, endDay, data)
        else:
            from saasapi_clickhouse.Funnel import Funnel
            funnel = Funnel()
            result = funnel.data(datatype, params)
        data = json.dumps(result).decode('unicode-escape').encode('utf8')
        return HttpResponse(data)
    except:
        import traceback
        exstr = traceback.format_exc()
        if DEBUG:
            print(exstr)
            return HttpResponse(exstr)
        else:
            print(exstr)

# 事件漏斗,线上
# 保留历史接口格式，不支持map属性查询
def getFunnelOld(request, datatype, s_tm, e_tm, events_quote):
    '''
    :param request:
    :param datatype:
    :param s_tm:
    :param e_tm:
    :param events_quote:
    :format url: http://101.201.145.120:8090/saasapi/eventserisesingle/feeling/2016-06-12/2016-06-17/[["in"], ["ac23", "ac11"], ["ac22"]]/
    :return:
    '''
    try:
        events_str = urllib.unquote(events_quote)
        events = json.loads(events_str)
        result = funneldataOld(datatype, s_tm, e_tm, events)
        # data = json.dumps(result, ensure_ascii=False)
        data = json.dumps(result).decode('unicode-escape').encode('utf8')
        return HttpResponse(data)
    except:
        import traceback
        exstr = traceback.format_exc()
        if DEBUG:
            print(exstr)
            return HttpResponse(exstr)
        else:
            print(exstr)


# 事件留存
def getEventsRemain(request, datatype, params):
    # ['dl', 'ac23']
    try:
        params = urllib.unquote(params)
        params = json.loads(params)

        # if datatype not in ["biqu", "BIQU_ANDROID", "biqu_all", "feeling", "ncf_h5", "guaeng", "caiyu_ad"]:
        if False:
            result = eventsRemainMap(datatype, params)
        else:
            from saasapi_clickhouse.EventRemain import EventRemain
            event_remain = EventRemain()
            result = event_remain.data(datatype, params)

        data = json.dumps(result).decode('unicode-escape').encode('utf8')
        return HttpResponse(data)
    except:
        import traceback
        exstr = traceback.format_exc()
        if DEBUG:
            print(exstr)
            return HttpResponse(exstr)
        else:
            print(exstr)


# 事件留存，（日期范围、新格式）
def getEventsRemainCombine(request, datatype, events_quote):
    # ['dl', 'ac23']
    try:
        data_str = urllib.unquote(events_quote)
        paramse = json.loads(data_str)
        begintm = paramse["startTime"]
        endtm = paramse["endTime"]
        events = paramse["events"]
        lastday = int(paramse["lastDay"])
        result = eventsRemainCombine(datatype, begintm, endtm, lastday, events)
        # data = json.dumps(result, ensure_ascii=False, sort_keys=True)
        data = json.dumps(result).decode('unicode-escape').encode('utf8')
        return HttpResponse(data)
    except:
        import traceback
        exstr = traceback.format_exc()
        if DEBUG:
            print(exstr)
            return HttpResponse(exstr)
        else:
            print(exstr)


def getSample(request, datatype, s_tm):
    try:
        result = userSample(datatype, s_tm)
        # data = json.dumps(result, ensure_ascii=False)
        # data = json.dumps(result, ensure_ascii=False)
        data = json.dumps(result).decode('unicode-escape').encode('utf8')
        return HttpResponse(data)
    except:
        import traceback
        exstr = traceback.format_exc()
        if DEBUG:
            print(exstr)
            return HttpResponse(exstr)
        else:
            print(exstr)


def getSample_lite(request, datatype, s_tm):
    try:
        result = userSample_lite(datatype, s_tm)
        # data = json.dumps(result, ensure_ascii=False)
        data = json.dumps(result).decode('unicode-escape').encode('utf8')
        return HttpResponse(data)
    except:
        import traceback
        exstr = traceback.format_exc()
        if DEBUG:
            print(exstr)
            return HttpResponse(exstr)
        else:
            print(exstr)


def search(request, datatype, dayStr, hour_s, hour_e, base_cond):
    try:
        base_cond = json.loads(base_cond)
        result = search_user(datatype, dayStr, int(hour_s), int(hour_e), base_cond)
        # data = json.dumps(result, ensure_ascii=False)
        data = json.dumps(result).decode('unicode-escape').encode('utf8')
        return HttpResponse(data, ensure_ascii=False)
    except:
        import traceback
        exstr = traceback.format_exc()
        if DEBUG:
            print(exstr)
            return HttpResponse(exstr)
        else:
            print(exstr)


# 用户抽样，搜索接口，线上
def rtSample(request, datatype, conds):
    try:
        import time
        a = time.time()
        print("satrt", "-"*100)
        base_cond = json.loads(conds)
        result = rt_search(datatype, _filter=base_cond)
        if len(result) == 0 and "jhd_userkey" in base_cond:
            uid = base_cond.pop("jhd_userkey")
            base_cond["jhd_pushid"] = uid
            result = rt_search(datatype, _filter=base_cond)
        # data = json.dumps(result, ensure_ascii=False)
        data = json.dumps(result).decode('unicode-escape').encode('utf8')
        print("end", "-"*100, time.time()-a)
        return HttpResponse(data)
    except:
        import traceback
        exstr = traceback.format_exc()
        if DEBUG:
            print(exstr)
            return HttpResponse(exstr)
        else:
            print(exstr)


# 事件交叉（跨天）,线上
def getEventsCrossCombine(request, datatype, s_tm, e_tm, last_tm, events_quote):
    try:
        events_str = urllib.unquote(events_quote)
        events = json.loads(events_str)
        last_tm = int(last_tm)
        result = eventsCrossCombine(datatype, s_tm, e_tm, last_tm, events)
        # data = json.dumps(result, ensure_ascii=False)
        data = json.dumps(result).decode('unicode-escape').encode('utf8')
        return HttpResponse(data)
    except:
        import traceback
        exstr = traceback.format_exc()
        if DEBUG:
            print(exstr)
            return HttpResponse(exstr)
        else:
            print(exstr)


# 事件交叉（支持map）,线上
def getCrossEventMap(request, datatype, params):
    try:
        params = urllib.unquote(params)
        params = json.loads(params)

        # if datatype not in ["biqu", "BIQU_ANDROID", "biqu_all", "feeling", "ncf_h5", "guaeng", "caiyu_ad"]:
        if False:
            result = crossEventMap(datatype, params)
        else:
            from saasapi_clickhouse.CrossEvent import CrossEvent
            cross_event = CrossEvent()
            result = cross_event.data(datatype, params)

        data = json.dumps(result).decode('unicode-escape').encode('utf8')

        return HttpResponse(data)
    except:
        import traceback
        exstr = traceback.format_exc()
        if DEBUG:
            print(exstr)
            return HttpResponse(exstr)
        else:
            # return HttpResponse(params)
            print(exstr)


def getEventSummary(request, datatype, conds):
    try:
        params = urllib.unquote(conds)
        params = json.loads(params)
        # result = eventSummary(datatype, params)

        # if datatype not in ["biqu", "BIQU_ANDROID", "biqu_all", "feeling", "ncf_h5", "guaeng", "caiyu_ad"]:
        if False:
            result = eventSummary(datatype, params)
        else:
            from saasapi_clickhouse.EventSummary import EventSummary
            event_summary = EventSummary()
            result = event_summary.data(datatype, params)

        data = json.dumps(result).decode('unicode-escape').encode('utf8')
        return HttpResponse(data)
    except:
        import traceback
        exstr = traceback.format_exc()
        if DEBUG:
            print(exstr)
            return HttpResponse(exstr)
        else:
            print(exstr)

def getRoundFlightInterval(request, params):
    try:
        from saasapi_clickhouse.RoundFlightInterval import RoundFlightInterval
        params = urllib.unquote(params)
        params = json.loads(params)
        query = RoundFlightInterval()
        result = query.query(params)
        data = json.dumps(result).decode('unicode-escape').encode('utf8')
        return HttpResponse(data)
    except:
        import traceback
        exstr = traceback.format_exc()
        if DEBUG:
            print(exstr)
            return HttpResponse(exstr)
        else:
            print(exstr)

def getRoundAirelines(request):
    try:
        from saasapi_clickhouse.RoundAireLine import RoundAireLine
        query = RoundAireLine()
        result = query.query()
        data = json.dumps(result).decode('unicode-escape').encode('utf8')
        return HttpResponse(data)
    except:
        import traceback
        exstr = traceback.format_exc()
        if DEBUG:
            print(exstr)
            return HttpResponse(exstr)
        else:
            print(exstr)



