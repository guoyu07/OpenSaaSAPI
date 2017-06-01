# -*- coding: utf-8 -*-
import __init__
import time
import sys
reload(sys)
sys.setdefaultencoding("utf-8")
from os import sys, path
import json
import datetime
import copy
from collections import OrderedDict
from ClickHouseClient.ClickHouseClient import ClickHouseClient
import logging
from Query import Query
from SaaSCommon.JHDecorator import fn_timer
import threading

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename=path.dirname(path.abspath(__file__)) + "/logs/" + "api.log",
                    filemode='a')

logger = logging.getLogger(__file__)

class Funnel(Query):
    '''
    输入：
    {
    "funnel":[[{"id":"jhddg_every"}],[{"id":"ac2"}],[{"id":"ac8","attrs":[{"id":"id","op":"like","val":"135"}]}],[{"id":"ac49","attrs":[{"id":"type","op":"nis","val":"经停"},{"id":"wf","op":"is","val":"0"},{"id":"st","op":"endswith","val":"0"}]}],[{"id":"ac50","attrs":[{"id":"name","op":"endswith","val":"亮"},{"id":"hbh","op":"startswith","val":"MU"}]}],[{"id":"ac53","attrs":[{"id":"op","op":"nis","val":"1"}]}],[{"id":"ac55","attrs":[{"id":"type","op":"is","val":"微信"}]}]],
    "endDay":"2017-01-12",
    "startDay":"2017-01-05",
    # 可选参数
    "attrs": {用户属性},
    }
    输出：
    {
    "2016-06-17": [
        1717,
        1105,
        1105
    ],
    "2016-06-16": [
        1753,
        1123,
        1123
    ]
    }
    '''

    def __init__(self):
        # 记录任意事件（jhddg_every）位置
        self.every_event_indexes = []

    # def create_query_sql(self, db_name, start_day, end_day, funnel, attrs=None):
    #     '''
    #     :param db_name: appkey/datatype
    #     :param start_day: 起始日期，格式：yyyy-mm-dd
    #     :param end_day: 结束日期，格式：yyyy-mm-dd
    #     :param funnel: 事件属性，格式：[ [{id:…,attrs:[{id:…,op:…,val:…},{map属性“且”关系}]},{事件id“或”关系}], …]
    #     :param attrs: 基础属性，如：版本(jhd_vr)，渠道(jhd_pb)..., 格式：{"jhd_pb": "appstore", "jhd_vr": "1.0"}
    #     :return:
    #     '''
    #     # where partition between toDate(toDateTime('%(start_day)s 00:00:00')) and toDate(toDateTime('%(end_day)s 00:00:00'))%(where)s
    #     if attrs is None:
    #         attrs = {}
    #     columns_format = "sum(num_%(step)d) as step_%(step)d"
    #     # query_format = "select partition, %(columns)s from ( \
    #     # select partition, %(sequenceMatch)s \
    #     # from （%(db_name)s.userevent) \
    #     # where partition between toDate('%(start_day)s') and toDate('%(end_day)s')%(_where)s \
    #     # group by partition, jhd_userkey) \
    #     # group by partition \
    #     # order by partition desc"
    #
    #     query_format = "select partition, %(columns)s from ( \
    #     select partition, %(sequenceMatch)s \
    #     from (" \
    #                    "select partition, jhd_userkey, events_optimes.1 jhd_eventId, events_optimes.2 jhd_opTime, events_optimes.3 jhd_map from(" \
    #                    "select partition, jhd_userkey, arrayMap(i -> (events[i], optimes[i], maps[i]), indexes) events_optimes from (" \
    #                    "select partition, jhd_userkey, arrayFilter((i, event) -> i>1?events[i]!=events[i-1]:1, arrayEnumerate(events), events) indexes, events, optimes, maps from (" \
    #                    "select partition, jhd_userkey, groupArray(jhd_eventId) events, groupArray(jhd_opTime) optimes, groupArray(jhd_map) maps from " \
    #                    "(select partition, jhd_userkey, jhd_eventId, jhd_opTime, case when jhd_eventId in (%(map_events)s) then jhd_map else '{}' end jhd_map from %(db_name)s.userevent where partition between toDate('%(start_day)s') and toDate('%(end_day)s')%(_where)s order by jhd_opTime) " \
    #                    "group by partition, jhd_userkey))) " \
    #                    "array join events_optimes" \
    #                    ") " \
    #     "group by partition, jhd_userkey) " \
    #     "group by partition " \
    #     "order by partition desc"
    #
    #     length, sequenceMatch = self.fragment_sequenceMatch(funnel)
    #     columns = ", ".join([columns_format % {"step": i+1} for i in range(0, length)])
    #     _where = " and " + self.fragment_where(attrs)
    #
    #     map_events = self.get_map_events(funnel)
    #
    #     query = query_format % {"columns": columns, "sequenceMatch": sequenceMatch,
    #                             "_where": (_where if bool(attrs) else " ") + " and " + self.fragment_events(funnel),
    #                             "start_day": start_day,
    #                             "end_day": end_day,
    #                             "db_name": db_name,
    #                             "map_events": "".join(["'", "', '".join(map_events), "'"])
    #                             }
    #     print query
    #     logger.info(query)
    #     return query

    # def create_query_sql(self, funnel, attrs=None):
    #     if attrs is None:
    #         attrs = {}
    #     columns_format = "sum(num_%(step)d) as step_%(step)d"
    #     query_format = "select %(columns)s from ( \
    #     select %(sequenceMatch)s \
    #     from %(db_name)s.userevent \
    #     %(where)s \
    #     group by jhd_userkey)"
    #     length, sequenceMatch = self.fragment_sequenceMatch(funnel)
    #     columns = ", ".join([columns_format % {"step": i+1} for i in range(0, length)])
    #     _where = "where " + self.fragment_where(attrs)
    #     query = query_format % {"columns": columns, "sequenceMatch": sequenceMatch, \
    #                             "where": _where if bool(attrs) else ""}
    #     logger.info(query)
    #     return query

    def create_query_sql(self, db_name, start_day, end_day, funnel, attrs=None):
        if attrs is None:
            attrs = {}
        columns_format = "sum(num_%(step)d) as step_%(step)d"
        query_format = "select partition, %(columns)s from ( \
        select partition, %(sequenceMatch)s \
        from %(db_name)s.userevent \
        %(where)s \
        group by partition, jhd_userkey) \
        group by partition"

        events = self.get_events(funnel)

        length, sequenceMatch = self.fragment_sequenceMatch(funnel)
        columns = ", ".join([columns_format % {"step": i+1} for i in range(0, length)])
        _where = "where " + self.fragment_where(attrs) + ' and' if self.fragment_where(attrs) else "where " + \
                 " partition between '%(start_day)s' and '%(end_day)s'" % {"start_day": start_day, "end_day": end_day} + \
                 " and jhd_eventId in (%(map_events)s)" % {"map_events": "".join(["'", "', '".join(events), "'"])}
        # query = query_format % {"columns": columns, "sequenceMatch": sequenceMatch, \
        #                         "where": _where if bool(attrs) else "", "db_name": db_name}
        query = query_format % {"columns": columns, "sequenceMatch": sequenceMatch, \
                                "where": _where, "db_name": db_name}
        logger.info(query)
        print query
        return query

    def fragment_events(self, funnel):
        events = set()
        for step in funnel:
            for event_info in step:
                event_id = event_info["id"]
                events.add(event_id)
        return " ".join(["jhd_eventId", "in", "("+"'"+"','".join(list(events))+"'"+")"])

    def get_map_events(self, funnel):
        events = set()
        for step in funnel:
            for event_info in step:
                event_id = event_info["id"]
                if "attrs" in event_info and len(event_info["attrs"]) > 0:
                    events.add(event_id)
        return list(events)

    def get_events(self, funnel):
        events = set()
        for step in funnel:
            for event_info in step:
                event_id = event_info["id"]
                events.add(event_id)
        return list(events)

    def get_users_query(self, db_name, start_day, end_day, attrs = None):
        if attrs == None:
            attrs = {}
        _where = " and " + self.fragment_where(attrs)
        sql_format = "select partition, count(DISTINCT(jhd_userkey)) as uv \
        from %(db_name)s.userevent \
        where (partition between toDate('%(start_day)s') and toDate('%(end_day)s'))%(_attrs_where)s \
        group by partition \
        order by partition"
        sql = sql_format % {"db_name": db_name, "start_day": start_day, "end_day": end_day, "_attrs_where": _where if bool(attrs) else " "}
        logger.info(sql)
        return sql


    # 生成 sequenceMatch 条件
    def fragment_sequenceMatch(self, funnel):
        # fragment_format = "sequenceMatch('(?1).*(?2).*(?3)')(jhd_opTime, jhd_eventId = jhd_eventId, jhd_eventId = 'ac86' or jhd_eventId = 'ac87', jhd_eventId = 'ac88') as num_0"
        # funnel format: [[{"id":"jhddg_every"}],[{"id":"ac2"}],[{"id":"ac8","attrs":[{"id":"id","op":"like","val":"135"}]}],[{"id":"ac49","attrs":[{"id":"type","op":"nis","val":"经停"},{"id":"wf","op":"is","val":"0"},{"id":"st","op":"endswith","val":"0"}]}],[{"id":"ac50","attrs":[{"id":"name","op":"endswith","val":"亮"},{"id":"hbh","op":"startswith","val":"MU"}]}],[{"id":"ac53","attrs":[{"id":"op","op":"nis","val":"1"}]}],[{"id":"ac55","attrs":[{"id":"type","op":"is","val":"微信"}]}]]
        # [
        #     {
        #         "id": "ac8",
        #         "attrs": [
        #             {
        #                 "id": "id",
        #                 "op": "like",
        #                 "val": "135"
        #             }
        #         ]
        #     }
        # ]
        sequenceMatch_format = "sequenceMatch('%(pattern)s')(jhd_opTime, %(cond)s) as num_%(step)d"
        sequenceMatch_lis = []
        pattern_format = "(?%(index)d)"
        cond_eventid_format = "jhd_eventId = '%(event_id)s'"
        cond_map_format = "%(visit_params)s(jhd_map, '%(mapkey)s') %(operator)s"
        cond_lis = []
        for step, funnel_step in enumerate(funnel):
            step_event_or = []
            # 生成每个事件的条件，及mapkey条件（如果有）
            for event_data in funnel_step:
                event_mapkey_and = []
                event_id = event_data["id"]
                # 对 jhddg_every 单独处理
                if event_id == "jhddg_every":
                    # jhd_eventId = jhd_eventId 触发 sequence_match_max_iterations > 1000000 的BUG
                    cond_eventid = "jhd_eventId LIKE %(event_id)s" % {"event_id": "'%%'"}
                    event_mapkey_and.append(cond_eventid)
                    if len(funnel_step) == 1:
                        if step not in self.every_event_indexes:
                            self.every_event_indexes.append(step)
                    continue
                else:
                    cond_eventid = cond_eventid_format % {"event_id": event_id}
                    event_mapkey_and.append(cond_eventid)
                # 生成mapkey的条件(如果有)。
                if "attrs" in event_data and event_data["attrs"]:
                    #  包含多个mapkey限制条件
                    for map_item in event_data["attrs"]:
                        mapkey = map_item["id"]
                        op = map_item["op"]
                        mapvalue = map_item["val"]
                        visit_params, operator = self.query_operator(op, mapvalue)
                        cond_map = cond_map_format % {"visit_params": visit_params, "mapkey": mapkey, "operator": operator}
                        event_mapkey_and.append(cond_map)
                # eventid + map 条件
                step_event_or.append("(" + " and ".join(event_mapkey_and) + ")")
            # 并列 或 关系
            if bool(step_event_or):
                cond_lis.append("(" + " or ".join(step_event_or) + ")")
        for index, item in enumerate(cond_lis):
            pattern_lis = [pattern_format % {"index": i+1} for i in range(0, index+1)]
            pattern = ".*".join(pattern_lis)
            # 补位，sequenceMatch至少包含两步。
            cond = ", ".join(cond_lis[:index+1]) if index != 0 else ", ".join(cond_lis[:index+1] + ["(jhd_eventId <> '')"])
            sequenceMatch = sequenceMatch_format % {"pattern": pattern, "cond": cond, "step": index+1}
            sequenceMatch_lis.append(sequenceMatch)
        return (len(sequenceMatch_lis), ", ".join(sequenceMatch_lis))

    def funnel_length(self, funnel):
        return self.fragment_sequenceMatch(funnel)[0]

    def run_query(self, datatype, params, result):
        try:
            params = params if isinstance(params, dict) else json.loads(params)
            start_day = params["startDay"]
            end_day = params["endDay"]
            funnel = params["funnel"]
            attrs = params.get("attrs", {})

            # 查询天数最多为60天
            tm_s_stamp = time.mktime(time.strptime(start_day, "%Y-%m-%d"))
            tm_e_stamp = time.mktime(time.strptime(end_day, "%Y-%m-%d"))
            num = (tm_e_stamp - tm_s_stamp) / 86400
            if num > 60 or num < 0:
                return {"errinfo": "日期跨度超出范围！"}
        except:
            import sys
            import traceback
            exc_type, exc_value, exc_traceback = sys.exc_info()
            errinfo = traceback.format_exception(exc_type, exc_value, exc_traceback)
            logger.error(json.dumps(errinfo))
            return {"errinfo": "传递参数错误！"}
        try:
            try:
                query = str(self.create_query_sql(datatype, start_day, end_day, funnel, attrs).decode("utf-8"))
            except:
                query = self.create_query_sql(datatype, start_day, end_day, funnel, attrs).decode("utf-8").encode("utf-8")
            funnel_length = self.funnel_length(funnel)
        except:
            import sys
            import traceback
            exc_type, exc_value, exc_traceback = sys.exc_info()
            errinfo = traceback.format_exception(exc_type, exc_value, exc_traceback)
            logger.error(json.dumps(errinfo))
            return {"errinfo": "生成查询错误！"}
        logger.info(query)
        try:
            client = ClickHouseClient()
            for row in client.select(datatype, query):
                day = row.partition.strftime("%Y-%m-%d")
                funnel_step_user = [eval("row.step_%d" % (step+1, )) for step in range(0, funnel_length)]
                result.setdefault(day, funnel_step_user)
            # 单独处理任意事件
            if len(self.every_event_indexes) != 0 and self.every_event_indexes[0] == 0:
                users_query = self.get_users_query(datatype, start_day, end_day, attrs)
                for row in client.select(datatype, users_query):
                    day = row.partition.strftime("%Y-%m-%d")
                    uv = row.uv
                    for index in self.every_event_indexes:
                        # 当任意事件在步骤一
                        if index == 0:
                            result[day].insert(index, uv)
                        # 当任意事件不在步骤一
                        else:
                            result[day].insert(index, result[day][index-1])
        except:
            import sys
            import traceback
            exc_type, exc_value, exc_traceback = sys.exc_info()
            errinfo = traceback.format_exception(exc_type, exc_value, exc_traceback)
            logger.error(json.dumps(errinfo))
            return {"errinfo": "查询错误！"}

    @fn_timer
    def data(self, datatype, params):
        result = OrderedDict([])
        _threads = []
        params = params if isinstance(params, dict) else json.loads(params)
        start_day = params["startDay"]
        end_day = params["endDay"]
        start_date = datetime.datetime.strptime(start_day, "%Y-%m-%d").date()
        end_date = datetime.datetime.strptime(end_day, "%Y-%m-%d").date()
        while start_date <= end_date:
            try:
                params_copy = copy.deepcopy(params)
                params_copy["startDay"] = start_date.strftime("%Y-%m-%d")
                params_copy["endDay"] = start_date.strftime("%Y-%m-%d")
                task = threading.Thread(target=self.run_query, args=(datatype, params_copy, result))
                task.start()
                _threads.append(task)
                if sum(map(lambda t: 1 if t.isAlive() else 0, _threads)) > 10:
                    for _t in _threads:
                        if _t.isAlive():
                            _t.join()
                            break
                start_date += datetime.timedelta(days=1)
            except:
                import sys
                import traceback
                exc_type, exc_value, exc_traceback = sys.exc_info()
                errinfo = traceback.format_exception(exc_type, exc_value, exc_traceback)
                logger.error(json.dumps(errinfo))
        for _thread in _threads:
            _thread.join()
        # 按日期降序排列
        result_sorted = OrderedDict(sorted(result.iteritems(), key = lambda item: item[0], reverse=True))
        return result_sorted

if __name__ == "__main__":
    # funnel = [[{"id":"jhddg_every"}, {"id":"ac2"}],[{"id":"ac37"}],[{"id":"ac8","attrs":[{"id":"id","op":"like","val":"135"}]}],[{"id":"ac49","attrs":[{"id":"type","op":"nis","val":"经停"},{"id":"wf","op":"eq","val":"0"},{"id":"st","op":"endswith","val":"0"}]}],[{"id":"ac50","attrs":[{"id":"name","op":"endswith","val":"亮"},{"id":"hbh","op":"startswith","val":"MU"}]}],[{"id":"ac53","attrs":[{"id":"op","op":"nis","val":"1"}]}],[{"id":"ac55","attrs":[{"id":"type","op":"is","val":"微信"}]}]]
    # funnel = [[{"id":"ac2"}],[{"id":"ac6"}],[{"id":"ac8"}],[{"id":"ac49"}],[{"id":"ac50"}],[{"id":"ac53"}],[{"id":"ac55"}]]
    # attrs = {"jhd_vr": "1.0.1", "jhd_pb": "appstore"}
    # query_info = {"funnel":[[{"id":"jhddg_every","name":"启动应用"}],[{"id":"index_tz","name":"index_tz"}]],"endDay":"2017-02-16","startDay":"2017-01-01"}
    query_info = {"funnel":[[{"id":"jhddg_every","name":"启动应用"}],[{"id":"h5_index_nav_ucenter","name":"首页下方点击“我的”按钮"}]],"endDay":"2017-05-08","startDay":"2017-05-01"}
    # query_info = {"funnel":[[{"id":"jhddg_every","name":"启动应用"}],[{"id":"zc_ac1","name":"未注册时点击按钮，提示”请先完成注册“时发送"}],[{"id":"zc_ac2","name":"注册流程，点击”获取验证码“按钮"},{"id":"zc_ac3","name":"点击“获取语音验证码”按钮"}],[{"id":"zc_ac4","attrs":[{"id":"type","op":"is","val":"ok","name":"type"}],"name":"注册流程，输入验证码，点击”注册“按钮，完成注册"}]],"endDay":"2017-05-09","startDay":"2017-05-01"}
    tester = Funnel()
    a = time.time()
    # print tester.create_query_sql("2017-01-11", "2017-01-18", funnel, attrs)
    print json.dumps(tester.data("ncf_ws", query_info), ensure_ascii=False)
    print time.time() - a
    # print tester.fragment_events(query_info["funnel"])
    # print json.dumps(tester.get_users_query("caiyu_ad", "2017-02-01", "2017-02-04"), ensure_ascii=False)
    # print tester.data("biqu", query_info)
    # print tester.create_query_sql(funnel)
