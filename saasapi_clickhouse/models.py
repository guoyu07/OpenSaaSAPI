from __future__ import unicode_literals
from django.db import models

from saasapi.models import api_cache

# Create your models here.


# class APICache(models.Model):
#     inserttm = models.DateTimeField()
#     appkey = models.CharField(max_length=512)
#     api_id = models.CharField(max_length=512)
#     params = models.TextField()
#     data = models.TextField()
#     enable = models.IntegerField()