from django.test import TestCase
from MongoDatas import conn

# Create your tests here.

str = '''[{"$match": {"$or": [{"tm": '2016-06-16', "item_count.in": {"$exists": true}}, \
{"tm": '2016-06-18', "item_count.ac23": {"$exists": true}}]}},\
{"$group": {"_id": '$jhd_userkey', "groupnum": {"$sum": 1}}},\
{"$match": {"groupnum": {"$gt": 1}}},\
{"$group": {"_id": 'all', "total": {"$sum": 1}}}]'''

import json

print str
json_obj = json.loads(str)

print json_obj