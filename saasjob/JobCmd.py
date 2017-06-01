# -*- coding: utf-8 -*-
import os
import sys
import time
import uuid
reload(sys)
sys.setdefaultencoding("utf-8")


class JobCmd(object):

    def __init__(self, cmd):
        self.jobid = None
        self.createstamp = time.time() # 任务创建时间
        self._cmd = cmd # 任务命令
        self.statuscode = 0 # 任务执行返回状态码，0 表示正常


    def set_jobid(self, jobid):
        self.jobid = jobid

    # 执行
    def run(self):
        # print("object id %s , task id is %s, will run %s" %(id(self), self.jobid, time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(time.time()))))
        try:
            self.statuscode = os.system(self._cmd)
        except:
            import traceback
            print(traceback.print_exc(), self._cmd)
            # exc_info = sys.exc_info()
            self.statuscode = -1
        if self.statuscode == -1:
            raise RuntimeError('''%s execute faild.''' % self._cmd)

    # 任务属性
    def jobattr(self):
        result = {
            "create_stamp": self.createstamp,
            "cmd": self._cmd,
            "job_id": self.jobid,
        }
        return result


def jobcmdcallable(jobcmdobj, *args, **kwargs):
    jobcmdobj.run()

if __name__ == "__main__":
    jobcmdcallable("ipconfig")
