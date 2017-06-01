# -*- coding: utf-8 -*-
from hashlib import md5


def create_degest(content):
    m = md5()
    m.update(content)
    return m.hexdigest()