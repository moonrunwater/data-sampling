#!/usr/bin/python3
# -*- coding: utf-8 -*-

import functools
import json
import logging
import time
import datetime
import decimal
from bson.objectid import ObjectId

import requests

logger = logging.getLogger(__name__)

now_time_millis = lambda: int(round(time.time() * 1000))
tostr = lambda obj: str(obj) if obj else ''

# json.dumps(obj, ensure_ascii=False, cls=JsonEncoderX)
# If specified, default should be a function that gets called for objects that can’t otherwise be serialized.
# It should return a JSON encodable version of the object or raise a TypeError.
# If not specified, TypeError is raised.

class JsonEncoderX(json.JSONEncoder):
    def default(self, o):
        if isinstance(o, decimal.Decimal):
            return float(o)
        if isinstance(o, (datetime.datetime, datetime.date)):
            return o.isoformat()
        if isinstance(o, ObjectId):
            return str(o)
        return json.JSONEncoder.default(self, o)

# 钉钉机器人消息
def dingding_robot(token, msg):
    content = msg if isinstance(msg, str) else json.dumps(msg, indent=4, ensure_ascii=False, cls=JsonEncoderX)
    url = 'https://oapi.dingtalk.com/robot/send?access_token=%s' % token
    dingding_json = {
            "msgtype": "text",
            "text": {
                "content": content
            },
            "at": {
                "atMobiles": [
                    # "1825718XXXX"
                ],
                "isAtAll": False
            }
        }
    rsp = requests.post(url, json=dingding_json)
    logger.debug('rsp=%s', None if rsp == None else rsp.text)

# user:password
# host:port
def build_config(hostport, userpwd):
    return {
        'host': hostport.split(':')[0],
        'port': int(hostport.split(':')[1]),
        'user': userpwd.split(':')[0],
        'password': userpwd.split(':')[1]
    }

class TimeWatcher:

    def __init__(self):
        self.reset()

    def logtime(self, stage):
        self.end = now_time_millis()
        logger.debug('%s -> execute: %sms', stage, self.end - self.start)
        self.start = self.end
    
    def reset(self):
        self.start = now_time_millis()
        self.end = self.start


def logtime(func):
    '''
    log.debug execute time(ms)
    '''
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        start = now_time_millis()
        try:
            return func(*args, **kwargs)
        finally:
            end = now_time_millis()
            logger.debug('func=%s, execute: %sms', func.__name__, end - start)
    return wrapper


def retry(times):
    '''
    retry func operation the given times
    '''
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            cur = 0
            while True:
                cur += 1
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    if cur < times:
                        logger.error('func=%s, error: %s', func.__name__, repr(e))
                    else:
                        raise e
        return wrapper
    return decorator
