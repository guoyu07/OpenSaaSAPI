# -*- coding: utf-8 -*-
from __init__ import configPath
from pymongo import MongoClient
from pymongo.operations import *
from pymongo import ASCENDING
from pymongo import DESCENDING
import random
import ConfigParser
import itertools


class PyMongoClient(object):

    cf = ConfigParser.ConfigParser()
    cf.read(configPath)
    mongo_ip = cf.get("mongodb", "mongo_ip")
    mongo_port = cf.getint("mongodb", "mongo_port")
    mongo_user = cf.get("mongodb", "mongo_user")
    mongo_passwd = cf.get("mongodb", "mongo_passwd")

    def __init__(self, mongo_id = 1, **kwargs):
        self.load_mongo_config(mongo_id)
        self._conn = self._getConn(**kwargs)

    def load_mongo_config(self, mongo_id = 1):
        from Config import mongo_server
        mongo_info = mongo_server[mongo_id]
        self._url = mongo_info["mongo_ip"]
        self._port = mongo_info["mongo_port"]
        self._user = mongo_info["mongo_user"]
        self._passwd = mongo_info["mongo_passwd"]

    def _getConn(self, **kwargs):
        if self._passwd and self._user:
            connect_string = "mongodb://%(user)s:%(pwd)s@%(ip)s:%(port)s/" % {
                "user": self._user.strip(),
                "pwd": self._passwd.strip(),
                "ip": self._url.strip(),
                "port": str(self._port).strip(),
            }
            return MongoClient(connect_string,
                               document_class=dict,
                               tz_aware=False,
                               connect=True,
                               maxPoolSize = kwargs.get("maxPoolSize", 10),
                               socketTimeoutMS = kwargs.get("socketTimeoutMS", 500000),
                               connectTimeoutMS = kwargs.get("connectTimeoutMS", 20000),
                               serverSelectionTimeoutMS = kwargs.get("serverSelectionTimeoutMS", 300000),
                               waitQueueTimeoutMS = kwargs.get("waitQueueTimeoutMS", 10000),
                               waitQueueMultiple = kwargs.get("waitQueueMultiple", 2),
                               socketKeepAlive = kwargs.get("socketKeepAlive", False)
                               )
        else:
            return MongoClient(self._url,
                               int(self._port),
                               document_class=dict,
                               tz_aware=False,
                               connect=True,
                               maxPoolSize=kwargs.get("maxPoolSize", 10),
                               socketTimeoutMS=kwargs.get("socketTimeoutMS", 500000),
                               connectTimeoutMS=kwargs.get("connectTimeoutMS", 20000),
                               serverSelectionTimeoutMS=kwargs.get("serverSelectionTimeoutMS", 300000),
                               waitQueueTimeoutMS=kwargs.get("waitQueueTimeoutMS", 10000),
                               waitQueueMultiple=kwargs.get("waitQueueMultiple", 2),
                               socketKeepAlive=kwargs.get("socketKeepAlive", False)
                               )

    def getConn(self):
        return self._conn

    def findElemIn(self, dbname, collectionname, key, contains, conds, projection, step = 10000):
        length_total = len(contains)
        for part, index in zip(itertools.count(0), range(0, length_total, step)):
            part_x = contains[part*step: (part+1)*step]
            conds.update({key: {"$in": part_x}})
            cur = self._conn[dbname][collectionname].find(conds, projection)
            for item in cur:
                yield item

    def storeDaily(self, data, dbname, tablename, remove_dict):
        self._conn[dbname][tablename].remove(remove_dict) #删除
        for key in data: #插入数据
            tmp = data[key]
            tmp["random"] = random.randint(0, 1000)
            self._conn[dbname][tablename].insert(tmp)

    def remove(self, dbname, tablename, remove_dict):
        self._conn[dbname][tablename].remove(remove_dict)  # 删除

    def bulkWrite(self, dbName, collectionName, requests):
        step = 300
        collect = self._conn[dbName][collectionName]
        num = len(requests)
        for part, index in zip(itertools.count(0), range(0, num, step)):
            part_x = requests[part*step: (part+1)*step]
            try:
                collect.bulk_write(part_x)
            except:
                return requests[part*step:]
        return
        # return collect.bulk_write(requests)

    def find(self, dbName, collectionName, selector):
        collect = self._conn[dbName][collectionName]
        return collect.find(selector)

    def createIndex(self, dbName, collectionName, index):
        # index format: [(key, sort), ...]
        _index = []
        assert type(index) == type([])
        for key, _sort in index:
            assert isinstance(_sort, int)
            if _sort >= 0:
                _sort = ASCENDING
            else:
                _sort = DESCENDING
            _index.append((key, _sort))
        self._conn[dbName][collectionName].create_index(_index, background=True)

    def dropIndex(self, dbName, collectionName, index):
        # index format: [(key, sort), ...]
        _index = []
        assert type(index) == type([])
        for key, _sort in index:
            assert isinstance(_sort, int)
            if _sort >= 0:
                _sort = "1"
            else:
                _sort = "-1"
            _index.append(key)
            _index.append(_sort)
        _index_name = "_".join(_index)
        self._conn[dbName][collectionName].drop_index(_index_name)

    def database_names(self):
        return self._conn.database_names()

    def collection_names(self, tablename):
        return self._conn[tablename].collection_names(include_system_collections=False)

if __name__ == "__main__":

    pmc = PyMongoClient()
    # pmc.dropIndex("biqu", "UserEvent", [("partition_date", -1)])
    # for item in pmc.findElemIn("jh", "UserIP", "_id", ["221.232.131.162"], {"city": {"$exists": True}}, {"_id": False}):
    #     print item
    # data = {
    #     "key1": {"values": ["sssss"]},
    #     "key2": {"values": [None, None]}
    #         }
    client = pmc.getConn()
    print(client.database_names())
    # print(pmc.collection_names("biqu"))
    # db.collection_names(include_system_collections=False)
    # a = pmc.storeDaily(data, "hbtv", "test", {})
    # try:
    #     print("dddd", type(a), next(a))
    # except StopIteration:
    #     print("ssssss")




