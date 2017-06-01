#coding=utf-8
from __init__ import configPath
import MySQLdb
import ConfigParser
import sys
reload(sys)
sys.setdefaultencoding("utf-8")


class MysqlClient(object):
    cf = ConfigParser.ConfigParser()
    cf.read(configPath)
    mysql_host = cf.get("mysqldb", "mysql_host")
    mysql_port = cf.getint("mysqldb", "mysql_port")
    mysql_user = cf.get("mysqldb", "mysql_user")
    mysql_passwd = cf.get("mysqldb", "mysql_passwd")

    def __init__(self, db="saas_server", host=mysql_host, port=mysql_port, user=mysql_user, passwd=mysql_passwd):
        self.db = db
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.con, self.cur = self._connectMysql

    @property
    def _connectMysql(self):
        conn = MySQLdb.connect(
                host=self.host,
                port=self.port,
                user=self.user,
                passwd=self.passwd,
                db=self.db,
                charset='utf8'
                )
        cur = conn.cursor()
        return conn, cur

    @property
    def connection(self):
        return self.con, self.cur

    def select(self, cmd):
        try:
            self.cur.execute(cmd)
            print("success: ", cmd)
        except:
            import traceback
            print(traceback.print_exc())
            print("faild: ", cmd)

        for item in self.cur.fetchall():
            yield item

    # def getAppkey(self):
    #     result = []
    #     sql = "select a.appkey, b.cdkey, a.plat from (select * from saas_meta.d_app where enable = 1 and (plat = 'android' or plat = 'ios')) a left join (select * from saas_meta.d_account where enable = 1) b on a.own = b.name_uid"
    #     self.cur.execute(sql)
    #     for item in self.cur.fetchall():
    #         appkey, dbname, plat = item[0], item[1], item[2]
    #         appkey = appkey
    #         result.append((dbname, appkey, plat))
    #     return result

    def getAppkey(self):
        result = []
        sql = "select a.appkey, b.cdkey, a.plat from (select * from saas_meta.d_app where enable = 1 and (plat = 'android' or plat = 'ios' or plat = 'feeling' or plat = 'all')) a left join (select * from saas_meta.d_account where enable = 1) b on a.own = b.name_uid"
        self.cur.execute(sql)
        for item in self.cur.fetchall():
            appkey, dbname, plat = item[0], item[1], item[2]
            # appkey = appkey.lower()
            result.append((dbname, appkey, plat))
        return result

    def getAppkey_h5(self):
        result = []
        sql = "select a.appkey, b.cdkey, a.plat from (select * from saas_meta.d_app where enable = 1 and plat = 'h5') a left join (select * from saas_meta.d_account where enable = 1) b on a.own = b.name_uid"
        self.cur.execute(sql)
        for item in self.cur.fetchall():
            appkey, dbname, plat = item[0], item[1], item[2]
            result.append((dbname, appkey, plat))
        return result

    def getAppkey_kwargs(self, **kwargs):
        result = []
        columns_name = ["appkey", "plat", "mongo_id", "cdkey"]
        sql = "SELECT appkey, plat, mongo_id, cdkey FROM saas_server.d_appkey WHERE enable = 1"
        self.cur.execute(sql)
        for item in self.cur.fetchall():
            column_filter = [True] * len(item)
            for index, column in enumerate(columns_name):
                if kwargs.get(column, None) != None:
                    if isinstance(kwargs[column], list):
                        if not any([item[index] == kwargs[column][i] for i in range(0, len(kwargs[column]))]):
                            column_filter[index] = False
                            break
                    elif item[index] != kwargs[column]:
                        column_filter[index] = False
                        break
            if all(column_filter):
                result.append(dict(zip(columns_name, item)))
        # 除去重复项
        result_project = []
        filter_container = set([])
        filter_keys = kwargs.get("filter_keys", ["appkey"])
        for item in result:
            elem = tuple([item[key] for key in filter_keys])
            if elem in filter_container:
                continue
            item.pop("mongo_id")
            result_project.append(item)
            filter_container.add(elem)
        return result_project

    def get_mongoid(self, appkey):
        sql = "SELECT appkey, plat, mongo_id, cdkey FROM saas_server.d_appkey " \
              "WHERE enable = 1 AND appkey = '%(appkey)s'" \
              % {"appkey": appkey}
        self.cur.execute(sql)
        mongo_ids = set([])
        for item in self.cur.fetchall():
            print item
            mongo_ids.add(int(item[2]))
        mongo_id_lis = list(mongo_ids)
        mongo_id_lis.sort()
        return mongo_id_lis

    def table_add_column(self, db_name, table_name, field_name, field_type="VARCHAR(255)"):
        sql = "ALTER TABLE %(db_name)s.%(table_name)s ADD %(field_name)s %(field_type)s" % {
            "db_name": db_name,
            "table_name": table_name,
            "field_name": field_name,
            "field_type": field_type,
        }
        self.cur.execute(sql)
        self.con.commit()

    def table_update(self, db_name, table_name):
        sql = "update %(db_name)s.%(table_name)s set relation = valuetype" % {
            "db_name": db_name,
            "table_name": table_name,
        }
        self.cur.execute(sql)
        self.con.commit()

    def table_rename_column(self, db_name, table_name, field_name, field_name_new):
        # ALTER TABLE 【表名字】 CHANGE 【列名称】【新列名称】
        sql = "ALTER TABLE %(db_name)s.%(table_name)s CHANGE `%(field_name)s` `%(field_name_new)s` VARCHAR(255)" % {
            "db_name": db_name,
            "table_name": table_name,
            "field_name": field_name,
            "field_name_new": field_name_new,
        }
        print sql
        self.cur.execute(sql)
        self.con.commit()

    def table_change_columntype(self, db_name, table_name, field_name, field_type):
        # ALTER TABLE 【表名字】 CHANGE 【列名称】【新列名称】
        sql = "ALTER TABLE %(db_name)s.%(table_name)s modify column `%(field_name)s` %(field_type)s" % {
            "db_name": db_name,
            "table_name": table_name,
            "field_name": field_name,
            "field_type": field_type,
        }
        print sql
        self.cur.execute(sql)
        self.con.commit()

    def table_delete_column(self, db_name, table_name, cond):
        # ALTER TABLE 【表名字】 CHANGE 【列名称】【新列名称】
        sql = "DELETE FROM %(db_name)s.%(table_name)s WHERE %(cond)s" % {
            "db_name": db_name,
            "table_name": table_name,
            "cond": cond,
        }
        print sql
        self.cur.execute(sql)
        self.con.commit()

    def table_delete_row(self, db_name, table_name, cond):
        sql = "DELETE FROM %(db_name)s.%(table_name)s WHERE %(cond)s" % {
            "db_name": db_name,
            "table_name": table_name,
            "cond": cond,
        }
        print sql
        self.cur.execute(sql)
        self.con.commit()

    def closeMysql(self):
        self.cur.close()
        self.con.close()


if __name__ == "__main__":
    import json
    tester = MysqlClient("saas_meta")
    # print(tester.con, tester.cur)
    plats = ["android", "ios", "all", "feeling"]
    print(json.dumps(tester.getAppkey_kwargs(plat=plats, filter_keys=["appkey", "cdkey"])))
    # print(tester.get_mongoid(appkey="biqu"))
    # for data in tester.getAppkey_kwargs(plat=plats):
    #     db_name = data["cdkey"]
    #     appkey = data["appkey"]
    #     plat = data["plat"]
    #     table_name = "%(appkey)s_%(plat)s_ref_source" % {"appkey": appkey, "plat": plat}
    #     # tester.table_delete_row(db_name, table_name, "tm >= '2017-02-05 16:00:00'")
    #     # tester.table_delete_column(db_name, table_name, "valuetype not in ('number', 'string')")
    #     # tester.table_change_columntype(db_name, table_name, field_name, "TEXT")
    #     tester.table_add_column(db_name, table_name, field_name="refname")
    #     # tester.table_update(db_name, table_name)
    #     # tester.table_rename_column(db_name, table_name, "option", "options")
    # tester.closeMysql()
