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


class AireType(object):

    @staticmethod
    def data():

        result = {"data": [
            {"id": "11000000000", "name": u"必去"},
            {"id": "21000000001", "name": u"骑鹅"},
            {"id": "21000000005", "name": u"美团"},
            {"id": "21000000006", "name": u"金色世纪"},
            {"id": "21000000007", "name": u"红橘"},
            {"id": "21000000008", "name": u"百度糯米"},
            {"id": "21000000009", "name": u"百拓"},
            {"id": "21000000010", "name": u"淘旅行"},
            {"id": "13581995464", "name": u"快意商旅"}]
        }

if __name__ == "__main__":
    tester = AireType.data()
    print tester