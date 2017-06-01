# -*- coding: utf-8 -*-
import MysqlClient
import json


class MysqlData(object):

    def __init__(self):
        self.client = MysqlClient.MysqlClient("customize")

    def getUserTimeDistributeParms(self):
        result = {}
        for item in self.client.select("select appkey, usertimedistribute from customize.d_customapp_params"):
            appkey, usertimedistribute = item
            usertimedistribute = json.loads(usertimedistribute)
            result.setdefault(appkey, usertimedistribute)
        return result

    def getCustomParms(self, datatype):
        sql = "select a.appkey, b.cdkey, a.plat from \
              (select * from saas_meta.d_app where appkey = '%(appkey)s' and enable = 1 and (plat = 'android' or plat = 'ios')) a \
              left join \
              (select * from saas_meta.d_account where enable = 1) b \
              on a.own = b.name_uid" % {"appkey": datatype}
        for item in self.client.select(sql):
            dbname, appkey, plat = item[1], item[0], item[2]
            return (dbname, appkey, plat)


if __name__ == "__main__":
    tester = MysqlData()
    print tester.getCustomParms("biqu")