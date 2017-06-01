# coding: utf-8
import urllib
import json

def ipinfo_sina(ip):
    if not ip:
        return ('unknown', 'unknown', 'unknown')
    ip = ip.replace("_", ".").strip('''"''')
    base_url = 'http://int.dpool.sina.com.cn/iplookup/iplookup.php?format=json&ip=%(ip)s'
    try:
        text = urllib.urlopen(base_url % {"ip": ip}).read()
        json_obj = json.loads(text)
        country = json_obj["country"]
        pro = json_obj["province"]
        city = json_obj["city"] if json_obj["city"] else pro
        result = (country if country else u"未知", pro if pro else u"未知", city if city else u"未知")
    except:
        result = (u'未知', u'未知', u'未知')
    return result


if __name__ == '__main__':
    import time
    a = time.time()
    tmp = ipinfo_sina('111.44.144.176')
    print tmp[0], tmp[1], tmp[2]
    print time.time() - a