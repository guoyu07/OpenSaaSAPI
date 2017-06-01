# -*- coding: utf-8 -*-
import json
import datetime
import logging
import copy
import functools
import threading
from os import path
from collections import OrderedDict

from SaaSCommon.Digest import create_degest
from SaaSCommon.TasksRunner import TasksRunner
from CacheData import get_data
from CacheData import save_data

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename=path.dirname(path.abspath(__file__)) + "/logs/" + "api.log",
                    filemode='a')

logger = logging.getLogger(__file__)

global tasks_runner
tasks_runner = TasksRunner()


def common_cache_decorator(api_id, result_transform = None, result_detransform = None, reverse = True):
    def common_cache_decorator_wraper(func):
        @functools.wraps(func)
        def func_wapper(*args, **kwargs):
            digest_dict = {}
            appkey = args[1]
            params = args[2]
            cache_data = {}
            ### Run Before Begin ###
            try:
                params = params if isinstance(params, dict) else json.loads(params)
                start_day = params["startDay"]
                end_day = params["endDay"]
                start_date = datetime.datetime.strptime(start_day, "%Y-%m-%d").date()
                end_date = datetime.datetime.strptime(end_day, "%Y-%m-%d").date()

                index_day = start_day
                while start_date <= end_date:

                    try:
                        params_copy = copy.deepcopy(params)
                        params_copy["startDay"] = start_date.strftime("%Y-%m-%d")
                        params_copy["endDay"] = start_date.strftime("%Y-%m-%d")
                        digest = create_degest(json.dumps(params_copy, separators=(',', ':')) + appkey + api_id)
                        digest_dict.setdefault(start_date.strftime("%Y-%m-%d"),
                                               [digest, json.dumps(params_copy, separators=(',', ':'))]
                                               )

                        cache_item = json.loads(get_data(digest=digest))

                        if start_date.strftime("%Y-%m-%d") in cache_item:

                            cache_data.update(**cache_item)
                            index_day = start_day

                            t = threading.Thread(target=func, args=(args[0], appkey, params_copy))
                            tasks_runner.put(t)

                        else:
                            t = threading.Thread(target=func, args=(args[0], appkey, params_copy))
                            tasks_runner.put(t)
                    except:
                        import sys
                        import traceback
                        exc_type, exc_value, exc_traceback = sys.exc_info()
                        errinfo = traceback.format_exception(exc_type, exc_value, exc_traceback)
                        logger.error(json.dumps(errinfo))
                    start_date += datetime.timedelta(days=1)
            except:
                import sys
                import traceback
                exc_type, exc_value, exc_traceback = sys.exc_info()
                errinfo = traceback.format_exception(exc_type, exc_value, exc_traceback)
                logger.error(json.dumps(errinfo))
            ### Run Before End ###

            params_copy = copy.deepcopy(params)
            params_copy["startDay"] = index_day
            params_copy["endDay"] = end_date.strftime("%Y-%m-%d")

            data = func(args[0], appkey, params_copy, **kwargs)

            if "errinfo" in data:
                cache_data = {}
            else:
                data = result_transform(data) if result_transform else data

            ### Run After Begin ###
            try:
                # 存数据库
                for key in data:

                    digest = digest_dict[key][0]
                    params_copy = digest_dict[key][1]
                    _data = json.dumps({key: data[key]}, separators=(',', ':'))

                    task = threading.Thread(target=save_data, args=(digest, appkey, api_id, params_copy, _data))
                    tasks_runner.put(task)
            except:
                import sys
                import traceback
                exc_type, exc_value, exc_traceback = sys.exc_info()
                errinfo = traceback.format_exception(exc_type, exc_value, exc_traceback)
                logger.error(json.dumps(errinfo))
            ### Run After End ###

            data.update(**cache_data)
            result_sorted = OrderedDict(sorted(data.iteritems(), key=lambda item: item[0], reverse=reverse))
            result_sorted = result_detransform(result_sorted) if result_detransform else result_sorted
            return result_sorted
        return func_wapper
    return common_cache_decorator_wraper