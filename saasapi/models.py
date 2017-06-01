from __future__ import unicode_literals

from django.db import models

# Create your models here.


class api_cache(models.Model):
    rowid = models.AutoField(primary_key=True)
    inserttm = models.DateTimeField()
    digest = models.CharField(max_length=512)
    appkey = models.CharField(max_length=512)
    api_id = models.CharField(max_length=512)
    params = models.TextField()
    data = models.TextField()
    enable = models.IntegerField()