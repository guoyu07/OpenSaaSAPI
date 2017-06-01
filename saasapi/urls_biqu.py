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
from saasapi import views

urlpatterns = [
    url(r'^eventserise/(?P<datatype>\w+)/(?P<s_tm>[\d-]+)/(?P<e_tm>[\d-]+)/(?P<events_quote>.+)/$', views.getEventsSeries),
    url(r'^eventserisesingle/(?P<datatype>\w+)/(?P<s_tm>[\d-]+)/(?P<e_tm>[\d-]+)/(?P<events_quote>.+)/$', views.getFunnelOld), # 事件漏斗
    url(r'^funnel/(?P<datatype>\w+)/(?P<params>.+)/$', views.getFunnel), # 事件漏斗(支持map),线上
    # url(r'^eventsremain/(?P<datatype>\w+)/(?P<s_tm>[\d-]+)/(?P<last_tm>\d+)/(?P<events_quote>.+)/$', views.getEventsRemain), # 事件留存
    url(r'^eventsremain/(?P<datatype>\w+)/(?P<params>.+)/$', views.getEventsRemain), # 事件留存(支持map)，线上
    url(r'^eventsremaincombine/(?P<datatype>\w+)/(?P<events_quote>.+)/$', views.getEventsRemainCombine), # 事件留存，(多天，新格式)
    url(r'^eventslast/(?P<datatype>\w+)/(?P<s_tm>[\d-]+)/(?P<e_tm>[\d-]+)/(?P<last_tm>\d+)/(?P<events_quote>.+)/$', views.getEventsCrossCombine), # 事件交叉（跨天）
    url(r'^crossevent/(?P<datatype>\w+)/(?P<params>.+)/$', views.getCrossEventMap), # 事件交叉（支持map）,线上
    url(r'^usersample/(?P<datatype>\w+)/(?P<s_tm>[\d-]+)/$', views.getSample),
    url(r'^usersample_lite/(?P<datatype>\w+)/(?P<s_tm>[\d-]+)/$', views.getSample_lite),
    url(r'^search/(?P<datatype>\w+)/(?P<dayStr>[\d-]+)/(?P<hour_s>\d{1,2})/(?P<hour_e>\d{1,2})/(?P<base_cond>\{.*\})/$', \
        views.search),
    url(r'^rtsample/(?P<datatype>\w+)/(?P<conds>\{.*\})/$', views.rtSample), # 用户抽样，搜索接口
    url(r'^eventsummary/(?P<datatype>\w+)/(?P<conds>\{.*\})/$', views.getEventSummary), # 汇总一段时间的事件数据
]
