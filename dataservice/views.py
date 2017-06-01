# coding: utf-8
from django.shortcuts import render
from django.http import HttpResponse
import json
import urllib

from jhddgapi.settings import DEBUG

from DevUsers import DevUsers

from IPList import IPAccess

# Create your views here.


def getDevUsers(request, params):
    # ['dl', 'ac23']
    try:
        if request.META.has_key('HTTP_X_FORWARDED_FOR'):
            ip = request.META['HTTP_X_FORWARDED_FOR']
        else:
            ip = request.META['REMOTE_ADDR']
        if ('0.0.0.0' not in IPAccess) and (ip not in IPAccess):
            return ["request deny!"]
    except:
        pass
    try:
        params_str = urllib.unquote(params)
        data = json.loads(params_str)
        iscache = data.get("iscache", False)
        result = DevUsers(data["appkey"], iscache)
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