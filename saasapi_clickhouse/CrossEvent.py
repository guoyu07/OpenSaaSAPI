# -*- coding: utf-8 -*-
import __init__
import time
import threading
from collections import OrderedDict
import datetime
from os import sys, path
import json
import logging
from Query import Query

from ClickHouseClient.ClickHouseClient import ClickHouseClient
from CacheDecorator import common_cache_decorator
from SaaSCommon.JHDecorator import fn_timer

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename=path.dirname(path.abspath(__file__)) + "/logs/" + "api.log",
                    filemode='a')

logger = logging.getLogger(__file__)


class CrossEvent(Query):

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
        # sql_format = "select appoint, count(distinct(userkey)) as uv from ( \
        sql_format = "select appoint, count(distinct userkey) as uv from ( \
        select appoint, userkey from ( \
        select jhd_userkey as userkey from %(db_name)s.userevent prewhere partition = toDate('%(start_day)s') and jhd_opType = 'action' and %(map_cond_init)s%(_where)s group by jhd_userkey) \
        any inner join \
        (select 'cur_day' as appoint, jhd_userkey as userkey from %(db_name)s.userevent prewhere (partition between toDate('%(start_day)s') and toDate('%(start_day)s')) and jhd_opType = 'action' group by userkey) \
        using userkey \
        union all \
        select appoint, userkey from( \
        select jhd_userkey as userkey from %(db_name)s.userevent prewhere partition = toDate('%(start_day)s') and jhd_opType = 'action' and %(map_cond_init)s%(_where)s group by jhd_userkey) \
        any inner join \
        (select 'remain_day' as appoint, jhd_userkey as userkey from %(db_name)s.userevent prewhere (partition between toDate('%(start_day_1)s') and toDate('%(end_day)s')) and jhd_opType = 'action' and %(map_cond_conversion)s group by userkey) \
        using userkey) \
        group by appoint \
        order by appoint"
        _where = " and " + self.fragment_where(attrs)
        map_cond_init = self.map_conds(events[0])
        map_cond_conversion = self.map_conds(events[1])
        query = sql_format % {
            "db_name": db_name,
            "start_day": start_day,
            "yyyymmdd": start_day.replace("-", ""),
            "start_day_1": start_day_1,
            "end_day": end_day,
            "_where": _where if bool(attrs) else " ",
            "map_cond_init": map_cond_init,
            "map_cond_conversion": map_cond_conversion
            }
        logger.info(query)
        return query

    def create_query_sql_bak(self, db_name, start_day, end_day, events, attrs=None):
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
        # sql_format = "select appoint, count(distinct(userkey)) as uv from ( \
        sql_format = "select appoint, sum(1) as uv from ( \
        select appoint, userkey from \
        (select jhd_userkey as userkey from %(db_name)s.userevent where partition = toDate('%(start_day)s') and %(map_cond_init)s%(_where)s group by jhd_userkey) \
        all left join \
        ( \
        select 'cur_day' as appoint, jhd_userkey as userkey from %(db_name)s.userevent where (partition between toDate('%(start_day)s') and toDate('%(start_day)s')) group by userkey \
        union all \
        select 'remain_day' as appoint, jhd_userkey as userkey from %(db_name)s.userevent where (partition between toDate('%(start_day_1)s') and toDate('%(end_day)s')) and %(map_cond_conversion)s group by userkey) \
        using userkey) \
        group by appoint \
        order by appoint"
        _where = " and " + self.fragment_where(attrs)
        map_cond_init = self.map_conds(events[0])
        map_cond_conversion = self.map_conds(events[1])
        query = sql_format % {
            "db_name": db_name,
            "start_day": start_day,
            "start_day_1": start_day_1,
            "end_day": end_day,
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

    @common_cache_decorator("cross_event")
    @fn_timer
    def data(self, datatype, params, interval = 0):
        result = OrderedDict([])
        try:
            tm_str_s = params["startDay"]
            tm_str_e = params["endDay"]
            events = params["events"]
            windows = int(params["windows"])
            start_day = datetime.datetime.strptime(tm_str_s, "%Y-%m-%d")
            end_day = datetime.datetime.strptime(tm_str_e, "%Y-%m-%d")
            attrs = params.get("attrs", {})
            num = (end_day - start_day).days
            if num > 60 or num < 0:
                return {"errinfo": "日期跨度超出范围！"}
            if windows > 60 or windows <= 0:
                return {"errinfo": "窗口期超出范围！"}
        except:
            import traceback
            exc_type, exc_value, exc_traceback = sys.exc_info()
            errinfo = traceback.format_exception(exc_type, exc_value, exc_traceback)
            logging.error(json.dumps(errinfo))
            return {"errinfo": "参数错误！"}
        try:
            threads = []
            while start_day <= end_day:
                try:
                    query = self.create_query_sql(datatype, start_day.strftime("%Y-%m-%d"), (start_day + datetime.timedelta(days=windows)).strftime("%Y-%m-%d"), events, attrs=attrs)
                    try:
                        query = str(query.decode("utf-8"))
                    except:
                        query = query.decode("utf-8").encode("utf-8")
                except:
                    import traceback
                    print traceback.print_exc()
                t = threading.Thread(target=self.submit, args=(datatype, query, result, start_day.strftime("%Y-%m-%d")))
                t.start()
                threads.append(t)
                time.sleep(interval)
                start_day += datetime.timedelta(days=1)
            for _thread in threads:
                _thread.join()
        except:
            import traceback
            exc_type, exc_value, exc_traceback = sys.exc_info()
            errinfo = traceback.format_exception(exc_type, exc_value, exc_traceback)
            logging.error(json.dumps(errinfo))
            return {"errinfo": "查询错误！"}
        result_sort = OrderedDict([])
        days = result.keys()
        days.sort(reverse=True)
        for day in days:
            result_sort.setdefault(day, result[day])
        return result_sort

    def submit(self, db_name, query, result, tm):
        client = ClickHouseClient()
        item = [0, 0]
        for row in client.select(db_name, query):
            result.setdefault(tm, [0, 0])
            appoint = row.appoint
            uv = row.uv
            if appoint == "cur_day":
                result[tm][0] = uv
            elif appoint == "remain_day":
                result[tm][1] = uv


if __name__ == "__main__":

    tester = CrossEvent()

    # data = {"events":[[[{"id":"jhddg_every"}]],[[{"id":"ac36","attrs":[{"id":"og","op":"is","val":"SHA"}]}]]],"windows":"4","endDay":"2017-02-04","startDay":"2017-02-01"}
    data = {"endDay":"2017-05-01","events":[[[{"id":"jhddg_every","name":"任意事件"}]],[[{"id":"jhddg_every","name":"任意事件"}]]],"startDay":"2017-04-24","windows":7}
    import time
    a = time.time()
    print json.dumps(tester.data("ncf_ws", data), ensure_ascii=False)
    print time.time() - a
