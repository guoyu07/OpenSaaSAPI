import ConfigParser

cf = ConfigParser.ConfigParser()
cf.read("Config.ini")

print(cf.sections())

print(cf.options("db"))

print(cf.items("db"))



cf.set("db", "db_pass", "xgmtes222222t")
cf.write(open("Config.ini", "w"))