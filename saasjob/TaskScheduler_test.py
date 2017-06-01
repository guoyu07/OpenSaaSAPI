# -*- coding: utf-8 -*-
import time
from pytz import utc
from apscheduler.schedulers.blocking import BlockingScheduler
from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.mongodb import MongoDBJobStore
# from apscheduler.jobstores.sqlalchemy import SQLAlchemyJobStore
# from apscheduler.jobstores.memory import MemoryJobStore
from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor

from JobCmd import jobcmdcallable
from JobCmd import JobCmd
from DBClient.PyMongoClient import PyMongoClient


global mongoclient
mongoclient = PyMongoClient().getConn()

jobstores = {
'mongo': MongoDBJobStore(collection = 'jobtest', database = 'saasjob', client=mongoclient),
# 'default': MemoryJobStore()
}
executors = {
'default': ThreadPoolExecutor(20),
'processpool': ProcessPoolExecutor(5)
}
job_defaults = {
'coalesce': False,
'misfire_grace_time': 1,
'max_instances': 10
}


def test_job():
    # a = scheduler.get_job(job_id="666", jobstore=jobstores["mongo"])
    # b = scheduler.get_jobs(jobstore=jobstores["mongo"])
    # print a, b
    print 'hello world'

global jobcmdobj
jobcmdobj = JobCmd("ipconfig")

def test_job_b():
    # a = scheduler.get_job(job_id="666", jobstore=jobstores["mongo"])
    # b = scheduler.get_jobs(jobstore=jobstores["mongo"])
    # print a, b
    for _job in scheduler.get_jobs():
        print type(_job.__getstate__()), _job.__getstate__()

# scheduler = BackgroundScheduler(jobstores=jobstores, executors=executors, job_defaults=job_defaults)
scheduler = BackgroundScheduler(jobstores=jobstores, executors=executors, job_defaults=job_defaults)
# scheduler.add_jobstore(jobstore=jobstores["mongo"], alias="mongo")
# scheduler = BackgroundScheduler(executors=executors, job_defaults=job_defaults)
# scheduler.add_job(test_job_b, 'interval', seconds=2, jobstore="mongo", executor="default")
# scheduler.add_job(jobcmdcallable, 'interval', seconds=2, jobstore="mongo", args=[jobcmdobj])
# scheduler.add_jobstore(jobstore=jobstores["mongo"], alias="mongo")

try:
    # test_job_b()
    # scheduler.add_job(test_job_b, 'interval', seconds=10, name="test_job_b", jobstore='mongo')
    print scheduler.get_jobs()
    scheduler.start(paused=True)
    print scheduler.get_jobs()
    time.sleep(200)
except SystemExit:
    pass


