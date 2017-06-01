# coding:utf-8
import sys
reload(sys)
sys.setdefaultencoding('utf8')

__author__ = "wuxi<wuxi@ifeng.com>"
__date__ = "$2014-9-23 14:31:01$"

def ip2long(strIP):
    ip = strIP.split('.')
    return (long(ip[0]) << 24) + (long(ip[1]) << 16) + (long(ip[2]) << 8) + long(ip[3])

def bsearch(longip, array, low, high):
    if low > high:
        return ()
    mid = long((low + high) / 2)
    midrecord = array[mid]
    if longip >= midrecord[0] and longip <= midrecord[1]:
        return (midrecord[2],)
    elif low == high:
        return ()
    elif longip > midrecord[0]:
        return bsearch(longip, array, low, mid)
    elif longip < midrecord[0]:
        return bsearch(longip, array, mid+1, high)
    else :
        return ()

def bsearch_more(longip, array, low, high):
    if low > high:
        return ()
    mid = long((low + high) / 2)
    midrecord = array[mid]
    if longip >= midrecord[0] and longip <= midrecord[1]:
        return (midrecord[2],midrecord[3])
    elif low == high:
        return ()
    elif longip > midrecord[0]:
        #return bsearch_more(longip, array, low, mid)
        return bsearch_more(longip, array, mid+1, high)
    elif longip < midrecord[0]:
        #return bsearch_more(longip, array, mid+1, high)
        return bsearch_more(longip, array, low, mid)
    else :
        return ()


def bsearch_for(longip, array, low, high):
    if low > high:
        return ()
    mid = long((low + high) / 2)
    midrecord = array[mid]
    if longip >= midrecord[0] and longip <= midrecord[1]:
        return (midrecord[3],)
    elif low == high:
        return ()
    elif longip > midrecord[0]:
        return bsearch_for(longip, array, low, mid)
    elif longip < midrecord[0]:
        return bsearch_for(longip, array, mid+1, high)
    else :
        return ()

def bsearch_formore(longip, array, low, high):
    if low > high:
        return ()
    mid = long((low + high) / 2)
    midrecord = array[mid]
    if longip >= midrecord[0] and longip <= midrecord[1]:
        return (midrecord[3],midrecord[4])
    elif low == high:
        return ()
    elif longip > midrecord[0]:
        return bsearch_formore(longip, array, low, mid)
    elif longip < midrecord[0]:
        return bsearch_formore(longip, array, mid+1, high)
    else :
        return ()

def getlocid(targetIP, iniarray, type = 'normal'):
    longip = ip2long(targetIP)
    locid = ()
    if type == 'normal':
        locid = bsearch(longip, iniarray, 0, len(iniarray)-1)
    elif type == 'more':
        locid = bsearch_more(longip, iniarray, 0, len(iniarray)-1)
    elif type == 'for_normal':
        locid = bsearch_for(longip, iniarray, 0, len(iniarray)-1)
    elif type == 'for_more':
        locid = bsearch_formore(longip, iniarray, 0, len(iniarray)-1)
    if locid == ():
        if type in ['more','for_more']:
            locid = ('Unknown','Unknown')
        else:
            locid = ('Unknown',)
    return locid

# ./ipdata.log
def load(iniFile):
    recordlist = []
    for line in open(iniFile):
        items = line.split()
        if len(items) != 5:
            continue
        recordlist.append([long(items[0]),long(items[1]),items[2],items[3],items[4]])
    return recordlist

if __name__ == "__main__":
    # iniarray = load("./ipdata.log")
    # result = getlocid("113.124.232.55", iniarray, type='for_more')
    # print(result)
    # for item in result:
    #     print(item)
    pass