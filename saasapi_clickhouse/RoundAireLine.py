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


class RoundAireLine(object):

    def query(self):

        query = '''SELECT DISTINCT (flight_seg)
        FROM airlines.rt_et_departed_round_trip
        WHERE (is_round_trip = 1)'''


        logger.info(query)

        result = {"data": []}
        client = ClickHouseClient()
        for row in client.select("airlines", query):
            if row.flight_seg not in result["data"]:
                result["data"].append(row.flight_seg)
        result["data"].sort()
        return result

if __name__ == "__main__":
    tester = RoundAireLine()
    print tester.query({"aireLine": "CGQ-SYX", "startDate": "2016-06-06", "endDate": "2016-06-14"})