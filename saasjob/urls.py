# coding: utf-8
"""jhddgapi URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/1.9/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  url(r'^$', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  url(r'^$', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.conf.urls import url, include
    2. Add a URL to urlpatterns:  url(r'^blog/', include('blog.urls'))
"""
from django.conf.urls import url
from django.contrib import admin
from django.conf.urls import include
from saasjob import views

urlpatterns = [
    url(r'^addjobs/(?P<params>\[\{.*\}\])/$', views.addJobs),
    url(r'^removejobs/(?P<params>\[.*\])/$', views.removeJobs),
    url(r'^modifyjobs/(?P<params>\[\{.*\}\])/$', views.modifyJobs),
    url(r'^modifyjobdata/(?P<params>\[\{.*\}\])/$', views.modifyJobData),
    url(r'^findjobs/(?P<params>\[.*\])/$', views.findJobs),
    url(r'^getalljobs/(?P<params>\[\{.*\}\])/$', views.getAllJobs),
    url(r'^pausejobs/(?P<params>\[.*\])/$', views.pauseJobs),
    url(r'^resumejobs/(?P<params>\[.*\])/$', views.resumeJobs),
]
