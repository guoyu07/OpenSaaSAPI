# --coding=utf8--
from __init__ import configPath
import ConfigParser
import psycopg2
import sys
reload(sys)
sys.setdefaultencoding("utf-8")


class PostgreSqlClient(object):
    cf = ConfigParser.ConfigParser()
    cf.read(configPath)
    host = cf.get("postgre", "host")
    port = cf.getint("postgre", "port")
    user = cf.get("postgre", "user")
    passwd = cf.get("postgre", "passwd")

    def __init__(self, db, host=host, port=port, user=user, passwd=passwd):
        self.db = db
        self.host = host
        self.port = port
        self.user = user
        self.passwd = passwd
        self.con, self.cur = self._connectPostgreSql

    @property
    def _connectPostgreSql(self):
        conn = psycopg2.connect(
            host=self.host,
            port=self.port,
            user=self.user,
            password=self.passwd,
            database=self.db
        )
        cur = conn.cursor()
        return conn, cur

    def query(self, cmd):
        command = str(cmd).split()[0]
        assert command.lower().startswith("select")
        self.cur.execute(cmd)
        for item in self.cur.fetchall():
            yield item

    def close(self):
        self.cur.close()
        self.con.close()

    # def deleteRecordsByTS(self, ts):
    #     # try:
    #     #     sql = "delete from " + appkey + "_h5_rt" + " where tm = %s"
    #     #     print(sql)
    #     #     print(cur.execute(sql, [day]))
    #     #     conn.commit()
    #     # except Exception, e:
    #     #     print(e,)
    #     #     conn.rollback()
    #     # finally:
    #     #     cur.close()
    #     #     conn.close()


if __name__ == '__main__':
    client = PostgreSqlClient("jh_10a0e81221095bdba91f7688941948a6")
    for item in client.query("select * from biqu_h5_event_detail"):
        print(item)
    import uuid, time, datetime
    print(uuid.uuid1())
    print(datetime.datetime.now())
    from SaaSMode.EventDetailH5 import H5EventDetail
    print(H5EventDetail().build())
