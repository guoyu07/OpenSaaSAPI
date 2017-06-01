# coding: utf-8
import IPtoAreaFinals
from __init__ import ipdataPath
# from SaaSConfig.config import ipdata_path
global initarry
initarry = None

def getLoc(ip):
    global initarry
    try:
        ip = ip.strip("\"")
    except:
        import traceback
        print(traceback.print_exc())
    if initarry is None:
        initarry = IPtoAreaFinals.load(ipdataPath)
    locs = IPtoAreaFinals.getlocid(ip, initarry, type = "for_more")
    return ("unknown", locs[0], locs[1])

# prov, city = IPtoAreaFinals.getlocid("8.8.8.8", initarry, type = "for_more")



if __name__ == "__main__":
    import os
    print(os.getcwd())
    loc = getLoc("101.201.145.120")
    print(type(loc), loc)
    print(loc[0])
    print(loc[1])
