# -*- coding: utf-8 -*-
import __init__
import ConfigParser
from infi.clickhouse_orm.database import Database



# 扩展create_merge_table方法
class SubDatabase(Database):

    def create_merge_table(self, model_class):
        self._send(model_class.create_merge_table_sql(self.db_name))


if __name__ == "__main__":
    from __init__ import configPath
    cf = ConfigParser.ConfigParser()
    cf.read(configPath)
    DB_URL = cf.get("clickhouse", "db_url")
    PASSWORD = cf.get("clickhouse", "password")
    db = SubDatabase(db_name="biqu", db_url=DB_URL, password=PASSWORD)