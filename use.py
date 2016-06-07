# -*- coding: utf-8 -*-
"""
Created on Tue Jun  7 15:18:35 2016

@author: xinruyue
"""

class Pipeline:
    
    """ construct pipeline for MongoDB Aggregation"""
    
    def __init__(self):
        self.pipeline = []
        
    def match(self,**arg):
        self.pipeline.append({"$match":self.__recover(arg)})
        return self
    
    def group(self,**arg):
        self.pipeline.append({"$group":self.__recover(arg)})
        return self
    
    def unwind(self,**arg):
        self.pipeline.append({"$unwind":self.__recover(arg)})
        return self
        
    def project(self,**arg):
        self.pipeline.append({"$project":self.__recover(arg)})
        return self
    
    def sort(self,**arg):
        self.pipeline.append({"$sort":self.__recover(arg)})
        return self    
    
    def limit(self,**arg):
        self.pipeline.append(self.__recover(arg))
        return self  
    
    def get(self):
        return self.pipeline
    
    #  '___'    -->  '$', for first '___'
    #  '__'   -->  '.'
    def __recover(self,arg):
        for k in arg.keys():
            arg[k.replace('___','$',1).replace('__','.')] = arg.pop(k)
        return arg
    
    def replace(self, old,  new):
        return eval(str(self.pipeline).replace(old, new))


# collection: datebase's collection
# pipeline: MongDB aggregate pipeline
# keys: key need to push or addToSet
# note: this aggregation assume unique '_id'.
def aggregate(collection,pipeline,*keys):
    result_list = []
    agg_list = list(collection.aggregate(pipeline))
    if len(agg_list) > 0:
        for key in keys:
            if key in agg_list[0]:
                result_list.append(agg_list[0][key])
            else:
                result_list.append([])
    else:
        for key in keys:
            result_list.append([])
    return result_list


def unpack(iterable):
    result = []
    for x in iterable:
        if hasattr(x, '__iter__'):
            result.extend(unpack(x))
        else:
            result.append(x)
    return result


def merge_dict(*dicts):
    res = dict()
    for d in dicts:
        res.update(d)
    return res
    # return dict(sum(map(dict.items, dicts), []))


def to_list(a):
    return a if type(a) is list else [a]


def percent(d1, d2, s=False):
    res = 0
    if type(d1) is int and type(d2) is int:
        res = round(d1*100.0/d2, 2) if d2 else 0
    elif type(d1) is list and type(d2) is list:
        res = round(len(d1)*100.0/len(d2), 2) if len(d2) else 0
    return str(res)+'%' if s else res