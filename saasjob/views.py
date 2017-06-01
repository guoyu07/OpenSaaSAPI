# -*- coding: utf-8 -*-
import json
import urllib

from django.shortcuts import render
from django.http import HttpResponse

from SchedulerManager import SchedulerManager

# Create your views here.

global sched
sched = SchedulerManager()


# 添加 Job 任务
def addJobs(request, params):
    '''

    :demo params: [{"cmd": "python /data/py/test/saasjob_test.py test1 %s >> /data/py/test/test1.log", "jobfrq": {"second": "*/10", "minute": "*", "hour": "*", "day": "*", "day_of_week": "*", "month": "*"}}]
    :demo url: http://101.201.145.120:8000/saasjob/addjobs/[{"cmd": "python /data/py/test/saasjob_test.py test1 >> /data/py/test/test1.log", "params": {"second": "*/2", "minute": "*", "hour": "*", "day": "*", "day_of_week": "*", "month": "*", "data": {"name": "task1", "desc": "test"}}}]/
    :return: result
    '''
    try:
        global sched
        unquote_params = urllib.unquote(params)
        data = json.loads(unquote_params)
        result = []
        for jobinfo in data:
            try:
                cmd = jobinfo["cmd"]
                params = jobinfo["params"]
                _result = sched.addCron(cmd, **params)
            except:
                import traceback
                print(traceback.print_exc())
                _result = False
            result.append(_result)
        return HttpResponse(json.dumps(result))
    except:
        import traceback
        exstr = traceback.format_exc()
        return HttpResponse(exstr)


# 删除 Job 任务
def removeJobs(request, params):
    '''
    :demo params: [{"job_id": "e11fdd4aa45b4c178aaddea8fbd3fe27"}]
    :demo url: http://101.201.145.120:8000/saasjob/removejobs/[{"job_id": "e11fdd4aa45b4c178aaddea8fbd3fe27"}]/
    '''
    try:
        global sched
        unquote_params = urllib.unquote(params)
        data = json.loads(unquote_params)
        result = []
        for job_id in data:
            try:
                _result = sched.removeFromCron(job_id)
            except:
                import traceback
                print(traceback.print_exc())
                _result = False
            result.append(_result)
        result = json.dumps(result)
        return HttpResponse(result)
    except:
        import traceback
        exstr = traceback.format_exc()
        return HttpResponse(exstr)


# 修改 Job 任务
def modifyJobs(request, params):
    '''
    :demo params: [{"job_id": "12bdf9bf2149440baa6e22723a489392", "freqargs": {"second": 5}}]
    :demo url:
    '''
    try:
        global sched
        unquote_params = urllib.unquote(params)
        data = json.loads(unquote_params)
        result = []
        for _data in data:
            job_id = _data["job_id"]
            if _data.get("freqargs", None):
                try:
                    freqargs = _data["freqargs"]
                    freqargs_result = sched.modifyJobFreq(job_id, freqargs)
                except:
                    import traceback
                    print(traceback.print_exc())
                    freqargs_result = False
            else:
                freqargs_result = True
            if _data.get("attrargs", None):
                try:
                    attrargs = _data["attrargs"]
                    attrargs_result = sched.modifyJobFreq(job_id, attrargs)
                except:
                    import traceback
                    print(traceback.print_exc())
                    attrargs_result = False
            else:
                attrargs_result = True
            result.append({"freqargs_result": freqargs_result, "attrargs_result": attrargs_result})
        return HttpResponse(json.dumps(result))
    except:
        import traceback
        exstr = traceback.format_exc()
        return HttpResponse(exstr)


# 查询 Job 任务
def findJobs(request, params):
    '''
    :demo params:
    :demo url:
    '''
    try:
        global sched
        unquote_params = urllib.unquote(params)
        data = json.loads(unquote_params)
        result = sched.findCronJob(data)
        print(type(result), result)
        return HttpResponse(json.dumps(result))
    except:
        import traceback
        exstr = traceback.format_exc()
        return HttpResponse(exstr)


# 查询 Job 任务
def getAllJobs(request, params):
    '''
    :demo params: [{}]
    :demo return: {}
    :demo url: http://101.201.145.120:8000/saasjob/getalljobs/[{}]/
    '''
    try:
        global sched
        unquote_params = urllib.unquote(params)
        data = json.loads(unquote_params)
        result = sched.getAllJobInfo()
        result = json.dumps(result)
        return HttpResponse(result)
    except:
        import traceback
        exstr = traceback.format_exc()
        return HttpResponse(exstr)


# 停止所有 Job 任务，与 start 相反
def stopallJob(request, params):
    '''
    :demo params:
    :demo url:
    '''
    try:
        global sched
        unquote_params = urllib.unquote(params)
        data = json.loads(unquote_params)
        iswait = data["iswait"]
        result = sched.stop(iswait=iswait)
        return HttpResponse(json.dumps({"result": result}))
    except:
        import traceback
        exstr = traceback.format_exc()
        return HttpResponse(exstr)

# 暂停 Job 任务
def pauseJobs(request, params):
    '''
    :demo params:  ["7c430b1507984a698c87b06eda965925"]
    :demo url: http://101.201.145.120:8000/saasjob/pausejobs/[%22e52f73e1a4214bdbb9fc820e2ed92b9b%22]/
    '''
    try:
        global sched
        unquote_params = urllib.unquote(params)
        job_ids = json.loads(unquote_params)
        # job_ids = data["job_ids"]
        result = {}
        for job_id in job_ids:
            _result = sched.pause_job(job_id)
            result.setdefault(job_id, _result)
        return HttpResponse(json.dumps(result))
    except:
        import traceback
        exstr = traceback.format_exc()
        return HttpResponse(exstr)

#  恢复 Job 任务
def resumeJobs(request, params):
    '''
    :demo params: ["7c430b1507984a698c87b06eda965925"]
    :demo url: http://101.201.145.120:8000/saasjob/resumejobs/[%22e52f73e1a4214bdbb9fc820e2ed92b9b%22]/
    '''
    try:
        global sched
        unquote_params = urllib.unquote(params)
        job_ids = json.loads(unquote_params)
        # job_ids = data["job_ids"]
        result = {}
        for job_id in job_ids:
            _result = sched.resume_job(job_id)
            result.setdefault(job_id, _result)
        return HttpResponse(json.dumps(result))
    except:
        import traceback
        exstr = traceback.format_exc()
        return HttpResponse(exstr)


#  设置/修改 job 名称
def modifyJobData(request, params):
    '''
    :demo params: [{"job_id": "0cc69faff14743fd93414e2186f0b339", "data": {"name": "task_update", "desc": "desc_update"}}]
    :demo url: http://101.201.145.120:8000/saasjob/modifyjobdata/[{"job_id": "0811b03c393c461790f922f9cc454c66", "data": {"name": "测试任务"}}]/
    '''
    try:
        global sched
        unquote_params = urllib.unquote(params)
        params_list = json.loads(unquote_params)
        result = []
        for item in params_list:
            job_id = item["job_id"]
            data = item["data"]
            _result = sched.modifyJobData(job_id, data)
            result.append(_result)
        return HttpResponse(json.dumps(result))
    except:
        import traceback
        exstr = traceback.format_exc()
        return HttpResponse(exstr)