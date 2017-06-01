from pymongo import MongoClient
from config import mongo_ip
from config import mongo_port
import json
import itertools
import time
from ipinfo import ipinfo_sina
from config import mongo_con_string


global conn
# conn = MongoClient(mongo_con_string)
conn = MongoClient(mongo_ip, mongo_port)

def search(datatype, col_name, conds):
    global conn
    for item in conn[datatype][col_name].find(conds).limit(10000):
        yield item
    yield {}

