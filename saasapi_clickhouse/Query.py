# -*- coding: utf-8 -*-


class Query(object):

    def __init__(self):
        pass

    # 生成 where 条件
    def fragment_where(self, attrs):
        '''
        :param attrs: {key: value}
        :return:
        '''
        fragment_format = "%s = '%s'"
        wheres = []
        for key in attrs.keys():
            if isinstance(attrs[key], list):
                attrs[key] = attrs[key][0]
            value = attrs[key]
            if not value:
                continue
            wheres.append(fragment_format % (key, value))
        return " and ".join(wheres)

    # 生成事件条件
    def query_operator(self, op, value):
        try:
            if op == "is":
                return ("visitParamExtractString", "= '%s'" % (str(value),))
            if op == "nis":
                return ("visitParamExtractString", "<> '%s'" % (str(value),))
            elif op == "like":
                return ("visitParamExtractString", "like '%%%s%%'" % (str(value),))
            elif op == "nlike":
                return ("visitParamExtractString", "not like '%%%s%%'" % (str(value),))
            elif op == "startswith":
                return ("visitParamExtractString", "like '%s%%'" % (str(value),))
            elif op == "endswith":
                return ("visitParamExtractString", "like '%%%s'" % (str(value),))

            elif op == "eq":
                return ("visitParamExtractFloat", "= %.10f" % (float(value),))
            elif op == "ne":
                return ("visitParamExtractFloat", "<> %.10f" % (float(value),))
            elif op == "lte":
                return ("visitParamExtractFloat", "<= %.10f" % (float(value),))
            elif op == "lt":
                return ("visitParamExtractFloat", "< %.10f" % (float(value),))
            elif op == "gte":
                return ("visitParamExtractFloat", ">= %.10f" % (float(value),))
            elif op == "gt":
                return ("visitParamExtractFloat", "> %.10f" % (float(value),))
        except:
            raise TypeError("op: %s, value: %s" % (str(op), str(value)))

        raise NotImplementedError("operator %s is invalid!" % (str(op), ))