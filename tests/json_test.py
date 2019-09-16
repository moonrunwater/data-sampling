#!/usr/bin/python3
# -*- coding: utf-8 -*-

import json
from decimal import Decimal


class Column:
    def __init__(self, name, data_type, last_max):
        self.name = name
        self.data_type = data_type
        self.last_max = last_max

class Task:
    def __init__(self, name, column, money):
        self.name = name
        self.column = column
        self.money = money

def obj2builtin(obj):
    dct = {}
    if isinstance(obj, Decimal):
        return float(obj)
    else:
        dct['__class__'] = obj.__class__.__name__
        dct['__module__'] = obj.__module__
        dct.update(obj.__dict__)
    return dct

def dict2obj(dct):
    if '__class__' not in dct:
        return dct

    class_name = dct.pop('__class__')
    module_name = dct.pop('__module__')
    module = __import__(module_name)
    clazz = getattr(module, class_name)
    args = dict((key, value) for key, value in dct.items())
    return clazz(**args)
        

def test_json():
    task = Task('task_1', Column('id', 'int', 29), Decimal(9.99))

    # If specified, default should be a function that gets called for objects that can’t otherwise be serialized.
    # It should return a JSON encodable version of the object or raise a TypeError.
    # If not specified, TypeError is raised.
    json_str = json.dumps(task, ensure_ascii=False, default=obj2builtin)
    print(json_str)

    # object_hook is an optional function that will be called with the result of any object literal decoded (a dict). 
    # The return value of object_hook will be used instead of the dict. 
    tt = json.loads(json_str, object_hook=dict2obj)
    print(tt.name)
    print(type(tt.column), tt.column)
    print(tt.column.name)
    print(tt.column.last_max)
    print(type(tt.money), tt.money)

if __name__ == "__main__":
    test_json()
