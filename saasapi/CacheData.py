# -*- coding: utf-8 -*-
import __init__
import datetime
import logging
import json
from os import path


from saasapi.models import api_cache
# from django.core.exceptions import ObjectDoesNotExist

import os,django
os.environ["DJANGO_SETTINGS_MODULE"] = "jhddgapi.settings"
django.setup()

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename=path.dirname(path.abspath(__file__)) + "/logs/" + "api.log",
                    filemode='a')

logger = logging.getLogger(__file__)


def get_data(**kwargs):
    try:
        item = api_cache.objects.raw("select rowid, data from saas_server.saasapi_api_cache where digest = '%(digest)s' and enable = 1 order by inserttm desc" % {"digest": kwargs["digest"]})
        for _item in item:
            return _item.data
    except:
        import sys
        import traceback
        exc_type, exc_value, exc_traceback = sys.exc_info()
        errinfo = traceback.format_exception(exc_type, exc_value, exc_traceback)
        logger.error(json.dumps(errinfo))
        return '{}'


# def save_data(digest, appkey, api_id, params, data):
def save_data(digest, appkey, api_id, params, data):
    try:
        obj = api_cache(inserttm=datetime.datetime.now(), digest=digest, appkey=appkey, api_id=api_id, params=params, data=data, enable=1)
        obj.save()
    except:
        import sys
        import traceback
        exc_type, exc_value, exc_traceback = sys.exc_info()
        errinfo = traceback.format_exception(exc_type, exc_value, exc_traceback)
        logger.error(json.dumps(errinfo))


if __name__ == "__main__":
    save_data("test", "test", "test", "test")