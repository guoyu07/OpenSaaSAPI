# -*- coding: utf-8 -*-
import __init__
# import time
from os import sys, path
import json
from collections import OrderedDict
from ClickHouseClient.ClickHouseClient import ClickHouseClient
import logging

logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s %(filename)s[line:%(lineno)d] %(levelname)s %(message)s',
                    datefmt='%a, %d %b %Y %H:%M:%S',
                    filename=path.dirname(path.abspath(__file__)) + "/logs/" + "api.log",
                    filemode='a')

logger = logging.getLogger(__file__)


class RoundFlightInterval(object):

    def query_bak(self, params):
        air_line = params["airLine"].strip()
        air_type = params["airType"].strip()

        start_date = params["startDate"].strip()
        end_date = params["endDate"].strip()

        channel = None
        if air_type == "order":
            channel = params.get("channel", None)
        if channel is None or channel == "all":
            query = '''select
                        %(airline_col)s
                        , toDayOfWeek(partition) week
                        , count() dall
                        , countIf(toDate(backDepTime) - toDate(depTime) = 0) d0
                        , countIf(toDate(backDepTime) - toDate(depTime) = 1) d1
                        , countIf(toDate(backDepTime) - toDate(depTime) = 2) d2
                        , countIf(toDate(backDepTime) - toDate(depTime) = 3) d3
                        , countIf(toDate(backDepTime) - toDate(depTime) = 4) d4
                        , countIf(toDate(backDepTime) - toDate(depTime) = 5) d5
                        , countIf(toDate(backDepTime) - toDate(depTime) = 6) d6
                        , countIf(toDate(backDepTime) - toDate(depTime) = 7) d7
                        , countIf(toDate(backDepTime) - toDate(depTime) = 8) d8
                        , countIf(toDate(backDepTime) - toDate(depTime) = 9) d9
                        , countIf(toDate(backDepTime) - toDate(depTime) = 10) d10
                        , countIf(toDate(backDepTime) - toDate(depTime) > 10) dgt10
                        from airlines.biqu_%(airType)s_round_trip_record
                        where (partition between toDate('%(startDate)s') and toDate('%(endDate)s')) and %(airLineCond)s
                        group by %(airline)s, week
                        order by %(airline)s, week''' % {
                                        "airLineCond": "airline" + " ".join(["!=", "''"]) if air_line == "all" else " ".join(["=", air_line]),
                                        "startDate": start_date,
                                        "endDate": end_date,
                                        "airType": air_type,
                                        "airline_col": "airline" if air_line != "all" else "'all' airline",
                                        "airline": "airline" if air_line != "all" else "'all'",
                                    }
        elif air_type == "order" and bool(channel):
            query = '''select
                %(airline_col)s
                , toDayOfWeek(partition) week
                , count() dall
                , countIf(toDate(backDepTime) - toDate(depTime) = 0) d0
                , countIf(toDate(backDepTime) - toDate(depTime) = 1) d1
                , countIf(toDate(backDepTime) - toDate(depTime) = 2) d2
                , countIf(toDate(backDepTime) - toDate(depTime) = 3) d3
                , countIf(toDate(backDepTime) - toDate(depTime) = 4) d4
                , countIf(toDate(backDepTime) - toDate(depTime) = 5) d5
                , countIf(toDate(backDepTime) - toDate(depTime) = 6) d6
                , countIf(toDate(backDepTime) - toDate(depTime) = 7) d7
                , countIf(toDate(backDepTime) - toDate(depTime) = 8) d8
                , countIf(toDate(backDepTime) - toDate(depTime) = 9) d9
                , countIf(toDate(backDepTime) - toDate(depTime) = 10) d10
                , countIf(toDate(backDepTime) - toDate(depTime) > 10) dgt10
                from airlines.biqu_%(airType)s_round_trip_record
                where (partition between toDate('%(startDate)s') and toDate('%(endDate)s')) and channel = '%(channel)s' and %(airLineCond)s
                group by %(airline)s, week
                order by %(airline)s, week''' % {
                "airLineCond": "airline" + " ".join(["!=", "''"]) if air_line == "all" else " ".join(["=", air_line]),
                "startDate": start_date,
                "endDate": end_date,
                "airType": air_type,
                "channel": channel,
                "airline_col": "airline" if air_line != "all" else "'all' airline",
                "airline": "airline" if air_line != "all" else "'all'",
            }
        logger.info(query)
        result = {"data": []}
        client = ClickHouseClient()
        for row in client.select("airlines", query):
            item = OrderedDict()
            item.setdefault("week", row.week)
            item.setdefault("dall", row.dall)
            item.setdefault("d0", row.d0/float(row.dall) if row.dall != 0 else 0.0)
            item.setdefault("d1", row.d1/float(row.dall) if row.dall != 0 else 0.0)
            item.setdefault("d2", row.d2/float(row.dall) if row.dall != 0 else 0.0)
            item.setdefault("d3", row.d3/float(row.dall) if row.dall != 0 else 0.0)
            item.setdefault("d4", row.d4/float(row.dall) if row.dall != 0 else 0.0)
            item.setdefault("d5", row.d5/float(row.dall) if row.dall != 0 else 0.0)
            item.setdefault("d6", row.d6/float(row.dall) if row.dall != 0 else 0.0)
            item.setdefault("d7", row.d7/float(row.dall) if row.dall != 0 else 0.0)
            item.setdefault("d8", row.d8/float(row.dall) if row.dall != 0 else 0.0)
            item.setdefault("d9", row.d9/float(row.dall) if row.dall != 0 else 0.0)
            item.setdefault("d10", row.d10/float(row.dall) if row.dall != 0 else 0.0)
            item.setdefault("dgt10", row.dgt10/float(row.dall) if row.dall != 0 else 0.0)
            result["data"].append(item)
        return json.dumps(result)

    def query(self, params):

        week_days = {
            1: u"周一",
            2: u"周二",
            3: u"周三",
            4: u"周四",
            5: u"周五",
            6: u"周六",
            7: u"周日",
        }

        air_line = params["airLine"].strip()
        air_type = params["airType"].strip()

        start_date = params["startDate"].strip()
        end_date = params["endDate"].strip()


        query = '''select
                    %(airline_col)s
                    , toDayOfWeek(partition) week
                    , count() dall
                    , countIf(toDate(backDepTime) - toDate(depTime) = 0) d0
                    , countIf(toDate(backDepTime) - toDate(depTime) = 1) d1
                    , countIf(toDate(backDepTime) - toDate(depTime) = 2) d2
                    , countIf(toDate(backDepTime) - toDate(depTime) = 3) d3
                    , countIf(toDate(backDepTime) - toDate(depTime) = 4) d4
                    , countIf(toDate(backDepTime) - toDate(depTime) = 5) d5
                    , countIf(toDate(backDepTime) - toDate(depTime) = 6) d6
                    , countIf(toDate(backDepTime) - toDate(depTime) = 7) d7
                    , countIf(toDate(backDepTime) - toDate(depTime) = 8) d8
                    , countIf(toDate(backDepTime) - toDate(depTime) = 9) d9
                    , countIf(toDate(backDepTime) - toDate(depTime) = 10) d10
                    , countIf(toDate(backDepTime) - toDate(depTime) > 10) dgt10
                    from airlines.biqu_%(airType)s_round_trip_record
                    where (partition between toDate('%(startDate)s') and toDate('%(endDate)s')) and %(airLineCond)s %(channelCond)s
                    group by %(airline)s, week
                    order by %(airline)s, week''' % {
                                    "airLineCond": "airline" + " ".join(["!=", "''"]) if air_line == "all" else "airline" + " ".join(["=", "'" + air_line + "'"]),
                                    "startDate": start_date,
                                    "endDate": end_date,
                                    "airType": "order" if air_type == "meituan" else air_type,
                                    "airline_col": "airline" if air_line != "all" else "'all' airline",
                                    "airline": "airline" if air_line != "all" else "'all'",
                                    "channelCond": " and channel = '21000000005' " if air_type == "meituan" else "",
                                }
        logger.info(query)
        result = {"data": []}
        client = ClickHouseClient()
        for row in client.select("airlines", query):
            item = OrderedDict()
            item.setdefault("week", week_days[row.week])
            item.setdefault("dall", row.dall)
            item.setdefault("d0", row.d0/float(row.dall) if row.dall != 0 else 0.0)
            item.setdefault("d1", row.d1/float(row.dall) if row.dall != 0 else 0.0)
            item.setdefault("d2", row.d2/float(row.dall) if row.dall != 0 else 0.0)
            item.setdefault("d3", row.d3/float(row.dall) if row.dall != 0 else 0.0)
            item.setdefault("d4", row.d4/float(row.dall) if row.dall != 0 else 0.0)
            item.setdefault("d5", row.d5/float(row.dall) if row.dall != 0 else 0.0)
            item.setdefault("d6", row.d6/float(row.dall) if row.dall != 0 else 0.0)
            item.setdefault("d7", row.d7/float(row.dall) if row.dall != 0 else 0.0)
            item.setdefault("d8", row.d8/float(row.dall) if row.dall != 0 else 0.0)
            item.setdefault("d9", row.d9/float(row.dall) if row.dall != 0 else 0.0)
            item.setdefault("d10", row.d10/float(row.dall) if row.dall != 0 else 0.0)
            item.setdefault("dgt10", row.dgt10/float(row.dall) if row.dall != 0 else 0.0)
            result["data"].append(item)
        for week_order in [1, 2, 3, 4, 5, 6, 7]:
            if week_days[week_order] not in [item["week"] for item in result["data"]]:
                result["data"].insert(week_order-1, {
                    "week": week_days[week_order],
                    "dall": 0,
                    "d0": 0.0,
                    "d1": 0.0,
                    "d2": 0.0,
                    "d3": 0.0,
                    "d4": 0.0,
                    "d5": 0.0,
                    "d6": 0.0,
                    "d7": 0.0,
                    "d8": 0.0,
                    "d9": 0.0,
                    "d10": 0.0,
                    "dgt10": 0.0
                })
        return result

if __name__ == "__main__":
    tester = RoundFlightInterval()
    print tester.query({"airLine": "all", "startDate": "2016-06-06", "endDate": "2017-06-14", "airType": "dep"})