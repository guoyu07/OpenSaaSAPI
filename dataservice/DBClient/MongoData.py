# -*- encoding: utf-8 -*-
from PyMongoClient import PyMongoClient
import time
import datetime
from SummaryDatatypes import DATATYPES

class MongoData(object):

    def __init__(self):
        self.client = PyMongoClient()

    # 新增
    def newcomerCount(self, yyyymmddhh, datatype, ver, pub, collectionName="UserProfile"):
        newcomer_num = 0
        ver = {"$exists": True} if ver == "all" else ver
        pub = {"$exists": True} if pub == "all" else pub
        start_tm = datetime.datetime.strptime(yyyymmddhh, "%Y%m%d%H").strftime("%Y%m%d%H%M%S")
        end_tm = (datetime.datetime.strptime(yyyymmddhh, "%Y%m%d%H") + datetime.timedelta(hours=1)).strftime("%Y%m%d%H%M%S")
        query = {"comepub": pub, "comever": ver, "firstLoginTime": {"$gte": start_tm, "$lt": end_tm}}
        newcomer_num = self.client.find(datatype, collectionName, query).count()
        return newcomer_num

    # 当天新增活跃趋势（按小时）
    def newcomerCount_bak(self, users, appkey, collectionName="UserProfile", num=1):
        users = list(set(users))
        partition_date = time.strftime("%Y%m%d", time.localtime(time.time()-86400*num))
        olduser_num = self.client.find(appkey, collectionName, {"jh_uid": {"$in": users}, "partition_date": partition_date}).count()
        newcomer_num = len(users) - olduser_num
        return newcomer_num

    def activeAddup(self, yyyymmddhh, datatype, ver, pub, collectionName="uvfile"):
        tm = "-".join([yyyymmddhh[:4], yyyymmddhh[4:6], yyyymmddhh[6:8]])
        ver = {"$exists": True} if ver == "all" else ver
        pub = {"$exists": True} if pub == "all" else pub
        activeuser_num = self.client.find(datatype, collectionName, {"tm": tm, "jhd_vr": ver, "jhd_pb": pub}).count()
        return activeuser_num

    def activeAddup_bak(self, yyyymmddhh, appkey, ver, pub, collectionName="UserEvent"):
        # query demo:
        # [
        #     {$match: {$and: [{"partition_date": "20160929"}, {"jhd_ts": {$lte: 1475121599000.0}}, {"jhd_vr": "2.1.2"}, {
        #     "jhd_pb": "appstore"}]}
        #       },
        #     {$group: {"_id": "$jhd_userkey"}},
        #     {$group: {"total": {$sum: 1}, "_id": "alluser"}}
        # ]

        partition_date_con = {"partition_date": "".join([yyyymmddhh[:4], yyyymmddhh[4:6], yyyymmddhh[6:8]])}
        end_ts = time.mktime(time.strptime(yyyymmddhh+"5959", "%Y%m%d%H%M%S")) * 1000
        ver_con = {"jhd_vr": {"$exists": True}} if ver == "all" else {"jhd_vr": ver}
        pub_con = {"jhd_pb": {"$exists": True}} if pub == "all" else {"jhd_pb": pub}
        ts_con = {"jhd_ts": {"$lte": end_ts}}
        query = []
        match = {"$match": {"$and": [ts_con, partition_date_con, ver_con, pub_con]}}
        group_1 = {"$group": {"_id": "$jhd_userkey"}}
        group_2 = {"$group": {"_id": "$all", "usercount": {"$sum": 1}}}
        query.append(match)
        query.append(group_1)
        query.append(group_2)
        conn = self.client.getConn()
        result = conn[appkey][collectionName].aggregate(query, allowDiskUse=True)
        for item in result:
            return item["usercount"]


    def newcomerAddup(self, yyyymmddhh, datatype, ver, pub, collectionName="UserProfile"):
        newcomers = 0
        ver = {"$exists": True} if ver == "all" else ver
        pub = {"$exists": True} if pub == "all" else pub
        yyyymmdd = yyyymmddhh[:8]
        hh = yyyymmddhh[-2:]
        start_tm = "".join([yyyymmdd, "00", "0000"])
        end_tm = "".join([yyyymmdd, hh, "5959"])
        newcomers += self.client.find(datatype, collectionName, {"firstLoginTime": {"$gte": start_tm, "$lte": end_tm}, "comever": ver, "comepub": pub}).count()
        return newcomers

if __name__ == "__main__":
    tester = MongoData()
    yyyymmdd = "2016083018"
    users = set(["sdfas", "10:48:B1:1A:A6:B7"])
    print tester.newcomerCount("2016100613", "biqu_all", "all", "all")
    print tester.activeAddup("2016100613", "biqu_all", "all", "all")
    # print(tester.newcomerCount(users, "hbtv"))
    # print(tester.newcomerAddup(yyyymmdd, "hbtv"))