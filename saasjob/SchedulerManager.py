# -*- coding: utf-8 -*-
from importlib import import_module
import uuid
import time
# import copy

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
# from abc import ABCMeta, abstractmethod

from JobCmd import JobCmd
from JobCmd import jobcmdcallable
from DBClient.PyMongoClient import PyMongoClient
global mongoclient
_mongoclient = PyMongoClient().getConn()


class SchedulerManager(object):

    # __metaclass__ = ABCMeta
    global _mongoclient

    def __init__(self):
        self.jobstores = {
            'mongo': MongoDBJobStore(collection='job1', database='saasjob', client=_mongoclient),
            'default': MemoryJobStore()
        }
        self.executors = {
            'default': ThreadPoolExecutor(1),
            'processpool': ProcessPoolExecutor(1)
        }
        self.job_defaults = {
            'coalesce': False,
            'misfire_grace_time': 1,
            'max_instances': 1
        }
        self._sched = BackgroundScheduler(jobstores=self.jobstores, executors=self.executors, job_defaults=self.job_defaults)
        # 添加 任务提交 事件监听
        self._sched.add_listener(self.when_job_submitted, EVENT_JOB_SUBMITTED)
        # 添加 任务执行完成 事件监听
        self._sched.add_listener(self.when_job_executed, EVENT_JOB_EXECUTED)
        # 添加 任务异常退出 事件监听
        self._sched.add_listener(self.when_job_crashed, EVENT_JOB_ERROR)
        self._jobs = {}
        self._jobhandlers = {} # format, key: jobid,  value: jobhandler
        self._jobs_key = ["name", "func", "args", "kwargs"]
        self.start()

    def cmd_valid(self, cmd):
        cmd = cmd.strip()
        if cmd.startswith("python"):
            return True
        else:
            return False

    def get_job_trigger(self, _job):
        # ('trigger', <CronTrigger (second='4', timezone='Asia/Shanghai')>)
        _trigger = self._get_job_attr(_job, "trigger")
        # options = ["%s='%s'" % (f.name, f) for f in self.fields if not f.is_default]
        if _trigger:
            return dict([(f.name, f.__str__()) for f in _trigger.fields if not f.is_default])
        else:
            return {}


    # 获取job属性
    def _get_job_attr(self, _job, attr):
        try:
            result = eval("_job.%s" % attr)
            return result
        except:
            import traceback
            print(traceback.print_exc())
            return None

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

    # 添加例行任务，crontab 格式
    def addCron(self, cmd, **params):
        try:
            create_jobid = uuid.uuid4().hex
            if not self.cmd_valid(cmd):
                return {"errinfo": "wrong cmd"}
            jobcmdobj = JobCmd(cmd)
            data = params.get("data", {})
            jobcmdobj.set_jobid(create_jobid)
            s = params.get("second", None) if params.get("second", None) != "*" else None
            m = params.get("minute", None) if params.get("minute", None) != "*" else None
            h = params.get("hour", None) if params.get("hour", None) != "*" else None
            d = params.get("day", None) if params.get("day", None) != "*" else None
            dw = params.get("day_of_week", None) if params.get("day_of_week", None) != "*" else None
            mnth = params.get("month", None) if params.get("month", None) != "*" else None
            y = params.get("year", None) if params.get("year", None) != "*" else None
            _job = self._sched.add_job(jobcmdcallable,
                                        'cron', year=y, month=mnth, day=d, day_of_week=dw, hour=h, minute=m, second=s,
                                        args=[jobcmdobj, data],
                                        executor="processpool",
                                        jobstore="mongo",
                                        id = create_jobid)
            self._jobhandlers.setdefault(create_jobid, JobHandler(_job))
            # 保存 job 属性
            return {"job_id": create_jobid}
        except:
            import traceback
            print(traceback.print_exc(), cmd, params)
            return False

    # 修改 job 属性
    def modifyJobAttr(self, job_id, **changes):
        try:
            _job = self._sched.modify_job(job_id=job_id, **changes)
            self._jobs[job_id] = _job
            if job_id in self._jobhandlers:
                self._jobhandlers[job_id].job = _job
            else:
                self._jobhandlers.setdefault(job_id, JobHandler(_job))
            return True
        except:
            import traceback
            print(traceback.print_exc(), job_id, changes)
            return False

    def modifyJobData(self, job_id, data):
        try:
            args = self._get_job_attr(self._jobhandlers[job_id].job, "args")
            # args_copy = [item for item in args]
            for key in data:
                args[1][key] = data[key]
            _job= self._sched.modify_job(job_id, args=args)
            self._jobs[job_id] = _job
            if job_id in self._jobhandlers:
                self._jobhandlers[job_id].job = _job
            else:
                self._jobhandlers.setdefault(job_id, JobHandler(_job))
            return True
        except:
            import traceback
            print(traceback.print_exc(), job_id, data)
            return False

    # 修改执行时间，crontab 格式
    def modifyJobFreq(self, job_id, cronargs):
        try:
            _job = self._sched.reschedule_job(job_id, trigger='cron', **cronargs)
            self._jobs[job_id] = _job
            if job_id in self._jobhandlers:
                self._jobhandlers[job_id].job = _job
            else:
                self._jobhandlers.setdefault(job_id, JobHandler(_job))
            return True
        except:
            import traceback
            print(traceback.print_exc(), job_id, cronargs)
            return False

    # 删除 job
    def removeFromCron(self, job_id):
        try:
            self._sched.remove_job(job_id)
            if job_id in self._jobhandlers:
                self._jobhandlers.pop(job_id)
            if job_id in self._jobs:
                self._jobs.pop(job_id)
            return True
        except:
            import traceback
            print(traceback.print_exc(), job_id)
            return False

    def job_exists(self, job_id):
        if job_id in self._jobhandlers or job_id in self._jobs:
            if job_id not in self._jobhandlers and job_id in self._jobs:
                self._jobhandlers[job_id] = JobHandler(self._jobs[job_id])
            elif job_id in self._jobhandlers and job_id not in self._jobs:
                self._jobs[job_id] = self._jobhandlers[job_id].job
            return True
        else:
            return False

    # 根据 job id 查询任务信息
    def findCronJob(self, job_ids):
        result = []
        _keys = [
            "cmd",
            "create_stamp",
            "is_running",
            "start_stamp",
            "hope_runtime",
            "is_success",
            "is_pause",
            "status",
            "name",
            "desc",
            "allowmodify"
        ]
        for job_id in job_ids:
            print("job_exists", self.job_exists(job_id))
            if self.job_exists(job_id):
                _jobhander = self._jobhandlers[job_id]
                job_info = _jobhander.jobhandlerattr
                cron_trigger = {}
                # cron_trigger = self.get_cron_trigger(_jobhander.job)
                tmp = {}
                tmp["job_id"] = job_id
                if job_info["is_running"]:
                    execute_time = time.time() - job_info["start_stamp"]
                    tmp["running_time"] = round(execute_time, 3)
                else:
                    tmp["running_time"] = round(job_info["hope_runtime"], 3)
                for key in _keys:
                    v = job_info.get(key, None)
                    if key == "is_running":
                        tmp["finished"] = False if job_info["is_running"] else True
                    else:
                        tmp[key] = v
                if tmp["finished"]:
                    tmp["completed_per"] = 1.0
                else:
                    tmp["completed_per"] = round(tmp["running_time"]/max([tmp["running_time"], tmp["hope_runtime"]]), 3)
                # del tmp["hope_runtime"]
                # del tmp["is_success"]
                # del tmp["is_pause"]
                tmp.pop("hope_runtime")
                tmp.pop("is_success")
                tmp.pop("is_pause")
                _result = dict(tmp, **cron_trigger)
                print("_result", _result)
                if _result["status"] == 3:
                    _result["completed_per"] = 0
                    _result["running_time"] = 0
                    _result["start_stamp"] = None
                result.append(_result)
            else:
                result.append({"job_id": job_id, "errinfo": "no exists"})
        return result

    def getAllJobInfo(self):
        try:
            result = self.findCronJob(set(self._jobhandlers.keys())|set(self._jobs.keys()))
            return result
        except:
            import traceback
            print(traceback.print_exc())
            return False

    def start_addition(self):
        for _job in self._sched.get_jobs():
            job_id = self._get_job_attr(_job, "id")
            self._jobs.setdefault(job_id, _job)

    def start(self):
        try:
            self._sched.start()
            self._sched.pause()
            self.start_addition()
            self._sched.resume()
            return True
        except:
            import traceback
            print(traceback.print_exc())
            return False

    def stop(self, iswait = True):
        try:
            self._sched.shutdown(wait=iswait)
            self._jobhandlers.clear()
            return True
        except:
            import traceback
            print(traceback.print_exc())
            return False

    def pause_job(self, job_id):
        try:
            self._sched.pause_job(job_id=job_id)
            self._jobhandlers[job_id].ispause = True
            self._jobhandlers[job_id].status = 3
            self._jobhandlers[job_id].isrunning = False
            return True
        except:
            import traceback
            print(traceback.print_exc())
            return False

    def resume_job(self, job_id):
        try:
            self._sched.resume_job(job_id=job_id)
            self._jobhandlers[job_id].ispause = False
            self._jobhandlers[job_id].status = 1
            return True
        except:
            import traceback
            print(traceback.print_exc())
            return False


class JobHandler(object):
    '''
    {"jobobj": None, "jobcmdobj": None, "isrunning": False, "hprunningtime": 0, "startstamp": 0, "endstamp": 0}
    '''
    def __init__(self, job = None, handler = None):

        self.jobobj = job
        if job:
            self.jobcmdobj = self.get_job_attr("args")[0]
            self.job_id = self.get_job_attr("id")
        else:
            self.jobcmdobj = None
            self.job_id = None
        # 变量初始化
        self.is_running = False
        self.end_stamp = 0
        self.start_stamp = 0
        self.hp_runningtime = 0
        self.is_crash = False
        self.is_pause = False
        self.is_success = True
        # job 状态码，
        # -1：初始状态,
        # 0： 异常，
        # 1：执行完成，等待下次调度,
        # 2：正在执行，
        # 3：暂停
        self.status = -1

    def when_job_submitted(self):
        self.is_running = True
        self.start_stamp = time.time()
        self.status = 2

    def when_job_executed(self):
        self.is_running = False
        self.end_stamp = time.time()
        if self.status != 0 and self.is_success == True:
            self.status = 1
            self.hp_runningtime = self.end_stamp - self.start_stamp

    def when_job_crashed(self):
        self.is_success = False
        self.is_running = False
        self.end_stamp = time.time()
        self.status = 0
        # self.hp_runningtime = self.end_stamp-self.start_stamp

    @property
    def job(self):
        return self.jobobj

    @job.setter
    def job(self, _job):
        self.jobobj = _job
        if _job:
            self.jobcmdobj = self.get_job_attr("args")[0]
            self.job_id = self.get_job_attr("id")
        else:
            self.jobcmdobj = None
            self.job_id = None
        if self.jobcmdobj:
            self.cmd = self.jobcmdobj._cmd

    @property
    def isrunning(self):
        return self.is_running

    @isrunning.setter
    def isrunning(self, is_running):
        self.is_running = is_running

    @property
    def issuccess(self):
        return self.is_success

    @issuccess.setter
    def issuccess(self, is_success):
        self.is_success = is_success

    @property
    def ispause(self):
        return self.is_pause

    @ispause.setter
    def ispause(self, is_pause):
        self.is_pause = is_pause

    @property
    def exception(self):
        return self.exceptioninfo

    @exception.setter
    def exception(self, ex):
        self.exceptioninfo = ex

    @property
    def startstamp(self):
        return self.startstamp

    @startstamp.setter
    def startstamp(self, start_stamp):
        self.start_stamp = start_stamp

    @property
    def endstamp(self):
        return self.endstamp

    @endstamp.setter
    def endstamp(self, end_stamp):
        self.end_stamp = end_stamp

    @property
    def hprunningtime(self):
        return self.hp_runningtime

    @property
    def runningtime(self):
        if self.is_running:
            return int(time.time() - self.start_stamp)
        else:
            return 0

    # 获取job属性
    def get_job_attr(self, attr):
        try:
            result = eval("self.jobobj.%s" % attr)
            return result
        except:
            import traceback
            print(traceback.print_exc())
            return None

    # 获取job属性
    def get_job_attrs(self, attrs):
        result = []
        for attr in attrs:
            _result = self.get_job_attr(attr)
            result.append(_result)
        return result

    @property
    def jobhandlerattr(self):

        if self.jobcmdobj:
            job_attr = self.jobcmdobj.jobattr()
        else:
            job_attr = {}

        jobhandler_attr = {
            "job_id": self.job_id, # job id
            "is_running": self.is_running, # 是否正在运行
            "is_pause": self.is_pause, # 是否暂停执行
            "is_success": self.is_success, # 是否暂停执行
            "start_stamp": self.start_stamp, # 最近一次开始执行时间
            "end_stamp": self.end_stamp, # 最近一次执行完成时间
            "hope_runtime": self.hp_runningtime, # 预计执行时间
            "status": self.status, # 预计执行时间
        }
        try:
            job_name_arg = self.get_job_attr("args")[1].get("name", [""]),  # 任务名称
            job_name = job_name_arg[0] if len(job_name_arg) == 1 else ""
            jobhandler_attr["name"] = job_name
        except:
            jobhandler_attr["name"] = ""
        try:
            desc_arg = self.get_job_attr("args")[1].get("desc", [""]),  # 任务说明
            desc = desc_arg[0] if len(desc_arg) == 1 else ""
            jobhandler_attr["desc"] = desc
        except:
            jobhandler_attr["desc"] = ""
        try:
            allowmodify_arg = self.get_job_attr("args")[1].get("allowmodify", [False])  # 是否允许修改
            print("allowmodify_arg", allowmodify_arg)
            allowmodify = allowmodify_arg[0] if len(allowmodify_arg) == 1 else False
            jobhandler_attr["allowmodify"] = allowmodify
        except:
            jobhandler_attr["allowmodify"] = False
        return dict(job_attr, **jobhandler_attr)


if __name__ == "__main__":
    tester = SchedulerManager()
    print tester._sched
    # print tester.convertStringToFunction("DBClient.PyMongoClient.PyMongoClient")
