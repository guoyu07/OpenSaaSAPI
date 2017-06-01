# -*- coding: utf-8 -*-
import time
from DBClient.MysqlClient import MysqlClient


global result
result = {}

global myclock
myclock = time.time()

def DevUsers(datatype='all', iscache = False):
    global result
    global myclock
    if ((time.time() - myclock) > 3600 or (not result)) or iscache:
        client = MysqlClient("saas_meta")
        con, cur = client.connection
        sql = "SELECT a.appkey, b.userkey FROM saas_exclude_appkey a LEFT JOIN saas_dev_userkey b on a.appkey = b.appkey WHERE a.`enable` = 1"
        cur.execute(sql)
        for item in cur.fetchall():
            appkey, userkey = item[0], item[1]
            result.setdefault(appkey, set()).add(userkey)
        myclock = time.time()
        client.closeMysql()
    else:
        pass
    tmp = {}
    for key in result:
        if "all" in datatype:
            tmp.setdefault(key, list(result[key]))
        elif key in datatype:
            tmp.setdefault(key, list(result[key]))
    return tmp

if __name__ == "__main__":
    print DevUsers()
