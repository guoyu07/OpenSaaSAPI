# -*- coding: utf-8 -*-
import __init__
import time
import datetime
import threading
from os import sys, path
import json
from ClickHouseClient.ClickHouseClient import ClickHouseClient
import logging
from Query import Query

from CacheDecorator import common_cache_decorator
from SaaSCommon.JHDecorator import fn_timer

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename=path.dirname(path.abspath(__file__)) + "/logs/" + "api.log",
                    filemode='a')

logger = logging.getLogger(__file__)


def result_transform(data):
    result = {}
    for item in data:
        tm = item["tm"]
        numbers = item["numbers"]
        length = item["length"]
        result.setdefault(tm, [numbers, length])
    return result

def result_detransform(data):
    result = []
    for key in data:
        tmp = {}
        tmp["tm"] = key
        tmp["numbers"] = data[key][0]
        tmp["length"] = data[key][1]
        result.append(tmp)
    return result


class EventRemain(Query):

    def __init__(self):
        pass

    def create_query_sql(self, db_name, start_day, end_day, events, attrs=None):
        '''
        :param db_name: appkey/datatype
        :param start_day: 起始日期，格式：yyyy-mm-dd
        :param end_day: 结束日期，格式：yyyy-mm-dd
        :param events: 事件id及map属性，格式：[ [ [{id:…,attrs:[{id:…,op:…,val:…},{mapkey}]},{或者关系}], [{并且关系}] ], [留存事件(格式同上)] ]
        :param attrs: 基础属性，如：版本(jhd_vr)，渠道(jhd_pb)..., 格式：{"jhd_pb": "appstore", "jhd_vr": "1.0"}
        :return:
        '''
        '''
        初始事件及转化事件没有实现“且”关系
        '''
        start_day_1 = (datetime.datetime.strptime(start_day, "%Y-%m-%d") + datetime.timedelta(days=1)).strftime("%Y-%m-%d")
        if attrs is None:
            attrs = {}
        sql_format = "select partition, count(distinct(userkey)) as uv from ( \
        select partition, userkey from \
        (select partition, jhd_userkey as userkey from %(db_name)s.userevent where partition = toDate('%(start_day)s') and %(map_cond_init)s%(_where)s group by partition, jhd_userkey) \
        all left join \
        (select partition, jhd_userkey as userkey from %(db_name)s.userevent where partition = toDate('%(start_day)s') or ((partition between toDate('%(start_day_1)s') and toDate('%(end_day)s')) and %(map_cond_conversion)s) group by partition, jhd_userkey) \
        using userkey) \
        group by partition \
        order by partition"
        _where = " and " + self.fragment_where(attrs)
        map_cond_init = self.map_conds(events[0])
        map_cond_conversion = self.map_conds(events[1])
        query = sql_format % {
            "db_name": db_name,
            "start_day": start_day,
            "start_day_1": start_day_1,
            "end_day": min(end_day, (datetime.datetime.today()-datetime.timedelta(days=1)).strftime("%Y-%m-%d")),
            "_where": _where if bool(attrs) else " ",
            "map_cond_init": map_cond_init,
            "map_cond_conversion": map_cond_conversion
            }
        logger.info(query)
        return query

    def map_conds(self, data):
        '''
        :param data: 格式：[ [{id:…,attrs:[{id:…,op:…,val:…},{mapkey}]},{或者关系}], [{并且关系}] ]
        :return:
        '''
        cond_eventid_format = "jhd_eventId = '%(event_id)s'"
        cond_map_format = "%(visit_params)s(jhd_map, '%(mapkey)s') %(operator)s"
        events_and = []
        for index, event_array in enumerate(data):
            events_or = []
            for event_data in event_array:
                event_solo_and = []
                event_id = event_data["id"]
                if event_id == "jhddg_every":
                    cond_eventid = "jhd_eventId != %(event_id)s" % {"event_id": "''"}
                    event_solo_and.append(cond_eventid)
                else:
                    event_solo_and.append(cond_eventid_format % {"event_id": event_id})
                if "attrs" in event_data and event_data["attrs"]:
                    #  包含多个mapkey限制条件
                    for map_item in event_data["attrs"]:
                        mapkey = map_item["id"]
                        op = map_item["op"]
                        mapvalue = map_item["val"]
                        visit_params, operator = self.query_operator(op, mapvalue)
                        cond_map = cond_map_format % {"visit_params": visit_params, "mapkey": mapkey, "operator": operator}
                        event_solo_and.append(cond_map)
                events_or.append("(" + " and ".join(event_solo_and) + ")")
            events_and.append(" or ".join(events_or))
        return " and ".join(events_and)

    @fn_timer
    # @common_cache_decorator("event_remain", result_transform, result_detransform, reverse=False)
    def data(self, datatype, params, interval = 0):
        result = []
        try:
            start_day = datetime.datetime.strptime(params["startDay"], "%Y-%m-%d")
            end_day = datetime.datetime.strptime(params["endDay"], "%Y-%m-%d")
            remain_num = int(params["remain"])
            events = params["events"]
            num = (end_day - start_day).days
            attrs = params.get("attrs", {})
            if num > 60 or num < 0:
                return {"errinfo": "日期跨度超出范围！"}
            if remain_num > 60 or remain_num <= 0:
                return {"errinfo": "窗口期超出范围！"}
        except:
            import sys
            import traceback
            exc_type, exc_value, exc_traceback = sys.exc_info()
            errinfo = traceback.format_exception(exc_type, exc_value, exc_traceback)
            logging.error(json.dumps(errinfo))
            return {"errinfo": "参数错误！"}

        try:
            threads = []
            while start_day <= end_day:
                try:
                    query = str(self.create_query_sql(datatype, start_day.strftime("%Y-%m-%d"), (start_day + datetime.timedelta(days=remain_num)).strftime("%Y-%m-%d"), events, attrs=attrs).decode("utf-8"))
                except:
                    query = self.create_query_sql(datatype, start_day.strftime("%Y-%m-%d"), (start_day + datetime.timedelta(days=remain_num)).strftime("%Y-%m-%d"), events, attrs=attrs).decode("utf-8").encode("utf-8")
                # query = self.create_query_sql(datatype, start_day.strftime("%Y-%m-%d"), (start_day + datetime.timedelta(days=remain_num)).strftime("%Y-%m-%d"), events, attrs=attrs)
                t = threading.Thread(target=self.submit, args=(datatype, query, result, start_day.strftime("%Y-%m-%d")))
                t.start()
                threads.append(t)
                time.sleep(interval)
                start_day += datetime.timedelta(days=1)
            for _thread in threads:
                _thread.join()
        except:
            import sys
            import traceback
            exc_type, exc_value, exc_traceback = sys.exc_info()
            errinfo = traceback.format_exception(exc_type, exc_value, exc_traceback)
            logging.error(json.dumps(errinfo))
            return {"errinfo": "查询错误！"}
        # 对“缺失”数据补位
        for item in result:
            item["length"] = remain_num + 1
            item["numbers"] = (item["numbers"] if "numbers" in item else []) + ([""]*(remain_num + 1 - len(item["numbers"] if "numbers" in item else [])))

            day = datetime.datetime.strptime(item["tm"], "%Y-%m-%d")
            yesterday = datetime.datetime.today() - datetime.timedelta(days=1)
            import copy
            remain_form = ['']*((yesterday - day).days + 1)
            remain_result = copy.deepcopy(item["numbers"])
            for index, (value, form) in enumerate(zip(remain_result, remain_form)):
                if value == "":
                    item["numbers"][index] = 0

        # 按日期升序排列
        result_sorted = sorted(result, key = lambda item: item["tm"])
        return result_sorted

    def submit(self, db_name, query, result, tm):
        client = ClickHouseClient()
        item = {"tm": tm}
        for row in client.select(db_name, query):
            tm = row.partition.strftime("%Y-%m-%d")
            item.setdefault("tm", tm)
            uv = row.uv
            item.setdefault("numbers", []).append(uv)
        if bool(item):
            result.append(item)


if __name__ == "__main__":
    import os
    os.linesep
    tester = EventRemain()

    # data = {"remain":20,"endDay":"2017-02-04","events":[[[{"id":"ac36"}]],[[{"id":"jhddg_every"}]]],"startDay":"2017-02-01"}
    data = {"remain":7,"endDay":"2017-02-09","events":[[[{"id":"jhddg_every","name":"任意事件"}]],[[{"id":"jhddg_every","name":"任意事件"}]]],"startDay":"2017-02-02"}
    print json.dumps(tester.data("BIQU_ANDROID", data), ensure_ascii=False)