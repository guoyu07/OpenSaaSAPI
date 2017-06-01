# -*- coding: utf-8 -*-
import os
import sys
import time
reload(sys)
sys.setdefaultencoding("utf-8")


class JobCmd(object):

    def __init__(self, cmd = None, job_id = None):
        self.cmd = cmd
        self.job_id = job_id
        self.create_stamp = time.time()

    def run(self, *args, **kwargs):
        try:
            self.cmd = kwargs["cmd"]
            status_code = os.system(self.cmd)
        except:
            import traceback
            print(traceback.print_exc(), self.cmd)
            # exc_info = sys.exc_info()
            status_code = -1
        if status_code == -1:
            raise RuntimeError('''%s execute faild.''' % self.cmd)

    @property
    def command(self):
        return self.cmd

    # 任务属性
    def jobattr(self):
        result = {
            "create_stamp": self.createStamp,
            "cmd": self.cmd,
            "job_id": self.jobid,
        }
        return result


def jobcmdcallable(jobcmdobj, *args, **kwargs):
    jobcmdobj.run(*args, **kwargs)

if __name__ == "__main__":
    jobcmdcallable("ipconfig")
