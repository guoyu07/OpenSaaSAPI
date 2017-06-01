# -*- coding: utf-8 -*-
import urllib
import json
from django.http import HttpResponse
from jhddgapi.settings import DEBUG
from saasapi_clickhouse.Funnel import Funnel
from saasapi_clickhouse.CrossEvent import CrossEvent
from saasapi_clickhouse.EventRemain import EventRemain
from saasapi_clickhouse.RoundFlightInterval import RoundFlightInterval
from saasapi_clickhouse.RoundAireLine import RoundAireLine


def get_funnel(request, datatype, params):
    try:
        funnel = Funnel()
        params = urllib.unquote(params)
        params = json.loads(params)
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


def get_cross_event(request, datatype, params):
    try:
        cross_event = CrossEvent()
        params = urllib.unquote(params)
        params = json.loads(params)
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
            print(exstr)


def get_event_remain(request, datatype, params):
    try:
        cross_event = EventRemain()
        params = urllib.unquote(params)
        params = json.loads(params)
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
            print(exstr)

def get_round_fligth_interval(request, params):
    try:
        query = RoundFlightInterval()
        params = urllib.unquote(params)
        params = json.loads(params)
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

def get_round_airelines(request):
    try:
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