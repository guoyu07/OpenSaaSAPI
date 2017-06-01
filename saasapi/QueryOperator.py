# -*- coding: utf-8 -*-


def query_operator(op, value):
    if op == "is":
        return value
    if op == "nis":
        return {"$ne": str(value)}
    elif op == "like":
        return {"$regex": str(value)}
    elif op == "nlike":
        return value
    elif op == "startswith":
        return {"$regex": "^"+str(value)}
    elif op == "endswith":
        return {"$regex": str(value)+"$"}

    elif op == "eq":
        return int(float(value))
    elif op == "ne":
        return {"$ne": int(float(value))}
    elif op == "lte":
        return {"$lte": int(float(value))}
    elif op == "lt":
        return {"$lt": int(float(value))}
    elif op == "gte":
        return {"$gte": int(float(value))}
    elif op == "gt":
        return {"$gt": int(float(value))}

    raise NotImplementedError("operator %s is invalid!" % (op, ))