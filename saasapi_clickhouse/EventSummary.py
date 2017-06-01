# -*- coding: utf-8 -*-
import __init__
import sys
reload(sys)
sys.setdefaultencoding("utf-8")
from os import sys, path
import time
import json
from collections import OrderedDict
from ClickHouseClient.ClickHouseClient import ClickHouseClient
import logging
from Query import Query
from SaaSCommon.JHDecorator import fn_timer

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename=path.dirname(path.abspath(__file__)) + "/logs/" + "api.log",
                    filemode='a')

logger = logging.getLogger(__file__)


class EventSummary(Query):

    def __init__(self):
        pass

    def create_query_sql(self, db_name, start_day, end_day, params, events, attrs=None):
        if bool(events) == False:
            events = []

        if attrs is None:
            attrs = {}

        query_format = "select jhd_eventId as _id, sum(1) as pv, count(distinct jhd_userkey) as uv from %(db_name)s.userevent \
                        where (partition between toDate('%(start_day)s') and toDate('%(end_day)s')) and jhd_eventId != ''%(_where)s \
                        group by jhd_eventId"
        _where = self.fragment_where(params, events)
        query = query_format % {
            "db_name": db_name,
            "start_day": start_day,
            "end_day": end_day,
            "_where": _where if bool(params) else " ",
        }
        logger.info(query)
        return query

    def fragment_where(self, attrs, events):
        fragments = []
        if bool(events):
            if len(events) == 1:
                events.append('')
            events_cond = "jhd_eventId in %(events_tuple)s" % {"events_tuple": str(tuple(map(str, events)))}
            fragments.append(events_cond)
        attrs_cond = super(EventSummary, self).fragment_where(attrs)
        fragments.append(attrs_cond)
        return " and " + " and ".join(fragments)

    @fn_timer
    def data(self, datatype, params):
        # 解析参数
        try:
            params = params if isinstance(params, dict) else json.loads(params)
            events = params.pop("events", [])
            start_day = params.pop("startDay")
            end_day = params.pop("endDay")
            attrs_map = params.pop("attrs", [])

            # 查询天数最多为60天
            tm_s_stamp = time.mktime(time.strptime(start_day, "%Y-%m-%d"))
            tm_e_stamp = time.mktime(time.strptime(end_day, "%Y-%m-%d"))
            num = (tm_e_stamp - tm_s_stamp) / 86400
            if num > 90 or num < 0:
                return {"errinfo": "日期跨度超出范围！"}
        except:
            import sys
            import traceback
            exc_type, exc_value, exc_traceback = sys.exc_info()
            errinfo = traceback.format_exception(exc_type, exc_value, exc_traceback)
            logging.error(json.dumps(errinfo))
            return {"errinfo": "传递参数错误！"}
        # 生成查询
        try:
            try:
                query = str(self.create_query_sql(datatype, start_day, end_day, params, events, attrs_map).decode("utf-8"))
            except:
                query = self.create_query_sql(datatype, start_day, end_day, params, events, attrs_map).decode("utf-8").encode("utf-8")
        except:
            import sys
            import traceback
            exc_type, exc_value, exc_traceback = sys.exc_info()
            errinfo = traceback.format_exception(exc_type, exc_value, exc_traceback)
            logging.error(json.dumps(errinfo))
            return {"errinfo": "生成查询错误！"}
        # 返回结果
        result = []
        try:
            client = ClickHouseClient()
            for row in client.select(datatype, query):
                key = row._id
                uv = row.uv
                pv = row.pv
                result.append({"_id": key, "uv": uv, "pv": pv})
        except:
            import sys
            import traceback
            exc_type, exc_value, exc_traceback = sys.exc_info()
            errinfo = traceback.format_exception(exc_type, exc_value, exc_traceback)
            logging.error(json.dumps(errinfo))
            return {"errinfo": "查询错误！"}
        return result

if __name__ == "__main__":
    tester = EventSummary()
    # query_info = {"startDay": "2017-02-01", "endDay": "2017-02-14", "events": ["ac36"], "jhd_pb": "appstore", "jhd_vr": "2.1.2", " jhd_opType": "action"}
    query_info = {"startDay": "2017-03-20", "endDay": "2017-03-26", "jhd_opType": "action", "events": ["pc_dh"]}
    print json.dumps(tester.data("ncf_h5", query_info), ensure_ascii=False)
