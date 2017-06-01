# -*- coding: utf-8 -*-
from importlib import import_module
import uuid
import time

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.jobstores.mongodb import MongoDBJobStore
from apscheduler.jobstores.memory import MemoryJobStore

from apscheduler.triggers.cron import CronTrigger
from apscheduler.events import (EVENT_SCHEDULER_STARTED, EVENT_SCHEDULER_SHUTDOWN, EVENT_SCHEDULER_PAUSED,
             EVENT_SCHEDULER_RESUMED, EVENT_EXECUTOR_ADDED, EVENT_EXECUTOR_REMOVED,
             EVENT_JOBSTORE_ADDED, EVENT_JOBSTORE_REMOVED, EVENT_ALL_JOBS_REMOVED,
             EVENT_JOB_ADDED, EVENT_JOB_REMOVED, EVENT_JOB_MODIFIED, EVENT_JOB_EXECUTED,
             EVENT_JOB_ERROR, EVENT_JOB_MISSED, EVENT_JOB_SUBMITTED, EVENT_JOB_MAX_INSTANCES)

from apscheduler.executors.pool import ThreadPoolExecutor, ProcessPoolExecutor

from JobHandler import JobHandler
from DBClient.PyMongoClient import PyMongoClient
global _mongoclient
_mongoclient = PyMongoClient().getConn()


class SchedulerManager(object):
    global _mongoclient

    def __init__(self):

        self._jobs = {}
        self._jobhandlers = {} # format, key: jobid,  value: jobhandler
        self.create_scheduler()
        self.start()

    def create_scheduler(self):
        self.jobstores = {
            'mongo': MongoDBJobStore(collection='job1', database='saasjob', client=_mongoclient),
            'default': MemoryJobStore()
        }
        self.executors = {
            'default': ThreadPoolExecutor(20),
            'processpool': ProcessPoolExecutor(5)
        }
        self.job_defaults = {
            'coalesce': False,
            'misfire_grace_time': 1,
            'max_instances': 1
        }
        self._sched = BackgroundScheduler(jobstores=self.jobstores, executors=self.executors, job_defaults=self.job_defaults)
        # 添加 任务提交 事件监听
        self._sched.add_listener(self.when_job_submitted, EVENT_JOB_SUBMITTED)
        # # 添加 任务执行完成 事件监听
        # self._sched.add_listener(self.when_job_executed, EVENT_JOB_EXECUTED)
        # 添加 任务退出 事件监听
        self._sched.add_listener(self.when_job_crashed, EVENT_JOB_EXECUTED | EVENT_JOB_ERROR)

    def when_job_submitted(self, event):
        try:
            job_id = event.job_id
            if job_id not in self._jobhandlers and job_id in self._jobhandlers:
                self._jobhandlers.setdefault(job_id, JobHandler(self._jobs[job_id]))
            jobhandler = self._jobhandlers[event.job_id]
            jobhandler.when_job_submitted()
            print("%s submitted at %s" % (event.job_id, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))))
        except:
            import traceback
            print(traceback.print_exc())

    def when_job_executed(self, event):
        try:
            if event.exception:
                job_id = event.job_id
                if job_id not in self._jobhandlers:
                    self._jobhandlers.setdefault(job_id, JobHandler(self._jobs[job_id]))
                jobhandler = self._jobhandlers[event.job_id]
                jobhandler.when_job_crashed()
                print("%s crashed at %s" % (event.job_id, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))))
            else:
                job_id = event.job_id
                if job_id not in self._jobhandlers:
                    self._jobhandlers.setdefault(job_id, JobHandler(self._jobs[job_id]))
                jobhandler = self._jobhandlers[event.job_id]
                jobhandler.when_job_executed()
                print("%s executed at %s" % (event.job_id, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))))
        except:
            import traceback
            print(traceback.print_exc())

    def when_job_crashed(self, event):
        try:
            if event.exception:
                job_id = event.job_id
                if job_id not in self._jobhandlers:
                    self._jobhandlers.setdefault(job_id, JobHandler(self._jobs[job_id]))
                jobhandler = self._jobhandlers[event.job_id]
                jobhandler.when_job_crashed()
                print("%s crashed at %s" % (event.job_id, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))))
        except:
            import traceback
            print(traceback.print_exc())

    def fresh_jobs(self):
        self._sched.pause()
        for _job in self._sched.get_jobs():
            job_id = self._get_job_attr(_job, "id")
            self._jobs.setdefault(job_id, _job)
        self._sched.resume()

    def sync_jobs(self, job_ids = set()):
        job_ids = (set(self._jobhandlers.keys()) | set(self._jobs.keys())) if len(job_ids) == 0 else job_ids
        for job_id in job_ids:
            if job_id in self._jobhandlers and job_id not in self._jobs:
                self._jobs[job_id] = self._jobhandlers[job_id]
            elif job_id not in self._jobhandlers and job_id in self._jobs:
                self._jobhandlers[job_id] = self._jobs[job_id]
        job_ids = set()
        return True

    def start(self):
        try:
            self._sched.start(paused=True)
            self.fresh_jobs()
            return True
        except:
            import traceback
            print(traceback.print_exc())
            return False