from django.shortcuts import render
from django.http import HttpResponse

from MongoDatas import eventsSeries

from MongoDatas import eventsSingle

from MongoDatas import eventsRemain

import urllib
import json

# Create your views here.

def getEventsSeries(request, datatype, s_tm, e_tm, events_quote):
    # ['dl', 'ac23']
    try:
        events_str = urllib.unquote(events_quote)
        events = json.loads(events_str)
        result = eventsSeries(datatype, s_tm, e_tm, events)
        data = json.dumps(result, ensure_ascii=False, sort_keys=True)
        return HttpResponse(data)
    except:
        import traceback
        exstr = traceback.format_exc()
        return HttpResponse(exstr)

def getEventsSeriesSingle(request, datatype, s_tm, e_tm, events_quote):
    # ['dl', 'ac23']
    try:
        events_str = urllib.unquote(events_quote)
        events = json.loads(events_str)
        result = eventsSingle(datatype, s_tm, e_tm, events)
        data = json.dumps(result, ensure_ascii=False, sort_keys=True)
        return HttpResponse(data)
    except:
        import traceback
        exstr = traceback.format_exc()
        return HttpResponse(exstr)

def getEventsRemain(request, datatype, s_tm, last_tm, events_quote):
    # ['dl', 'ac23']
    try:
        events_str = urllib.unquote(events_quote)
        events = json.loads(events_str)
        result = eventsRemain(datatype, s_tm, int(last_tm), events)
        data = json.dumps(result, ensure_ascii=False, sort_keys=True)
        return HttpResponse(data)
    except:
        import traceback
        exstr = traceback.format_exc()
        return HttpResponse(exstr)


