import time

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
        self.end_stamp = 0
        self.start_stamp = 0
        self.hp_runningtime = 0

        self.is_finished = False
        self.is_crashed = False
        self.is_paused = False

        '''
        # job 状态码，
        # -1：等待调度,
        # 0： 异常，
        # 1：执行完成，等待下次调度,
        # 2：正在执行，
        # 3：暂停
        转换路径：
                    0 -> 2
                    1 -> 2, 1 -> 3
                    2 -> 3, 2 -> 1, 2 -> 0
                    3 -> -1(1), 3 -> 2
        '''
        self.status = 1

    def when_job_submitted(self):
        self.start_stamp = time.time()

        self.is_finished = False
        self.status = 2

    def when_job_executed(self):
        self.end_stamp = time.time()

        self.is_finished = True
        self.is_success = True
        if self.status == 2:
            self.status = 1
            self.hp_runningtime = self.end_stamp - self.start_stamp

    def when_job_crashed(self):
        self.end_stamp = time.time()

        self.is_success = False
        self.is_finished = True
        self.is_crash = True
        self.status = 0

    def when_job_resume(self):
        # from: 3
        if self.status == 3:
            self.status = 1

    def when_job_pause(self):
        # from: 0, 1, 2, 3
        self.status = 3

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
            self.cmd = self.jobcmdobj.command

    @property
    def isRunning(self):
        return self.is_running

    # @isRunning.setter
    # def isRunning(self, is_running):
    #     self.is_running = is_running

    @property
    def isSuccess(self):
        return self.is_success

    # @isSuccess.setter
    # def isSuccess(self, is_success):
    #     self.is_success = is_success

    @property
    def isPause(self):
        return self.is_pause

    # @isPause.setter
    # def isPause(self, is_pause):
    #     self.is_pause = is_pause

    @property
    def exception(self):
        return self.exceptioninfo

    # @exception.setter
    # def exception(self, ex):
    #     self.exceptioninfo = ex

    @property
    def startStamp(self):
        return self.startstamp

    # @startStamp.setter
    # def startStamp(self, start_stamp):
    #     self.start_stamp = start_stamp

    @property
    def endStamp(self):
        return self.endstamp

    # @endStamp.setter
    # def endStamp(self, end_stamp):
    #     self.end_stamp = end_stamp

    @property
    def hpRunTime(self):
        return self.hp_runningtime

    @property
    def runnTime(self):
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

    # 获取 trriger 属性
    def get_job_trigger(self):
        # ('trigger', <CronTrigger (second='4', timezone='Asia/Shanghai')>)
        _trigger = self.get_job_attr("trigger")
        # options = ["%s='%s'" % (f.name, f) for f in self.fields if not f.is_default]
        if _trigger:
            return dict([(f.name, f.__str__()) for f in _trigger.fields if not f.is_default])
        else:
            return {}

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