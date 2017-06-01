### mongodb
# mongo_ip = "172.16.152.148"
# mongo_ip = "10.45.141.35"
# mongo_ip = "10.45.141.35"
mongo_ip = "101.201.145.120"
mongo_port = 27017

mongo_user = ""
mongo_pwd = ""

# 'mongodb://tanteng:123456@localhost:27017/'
mongo_con_string = "mongodb://%(mongo_user)s:%(mongo_pwd)s@%(mongo_ip)s:%(mongo_port)s/" % \
                   {"mongo_ip": mongo_ip, "mongo_port": mongo_port, \
                    "mongo_user": mongo_user, "mongo_pwd": mongo_pwd}


if __name__ == "__main__":
    print mongo_con_string
    from pymongo import MongoClient
    find = {"tm": "2016-07-31"}
    con = MongoClient(mongo_con_string)