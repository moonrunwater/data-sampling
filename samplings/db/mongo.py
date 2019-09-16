#!/usr/bin/python3
# -*- coding: UTF-8 -*-

import datetime
import hashlib
import json
import logging
import os

import pymongo
from bson.objectid import ObjectId

from samplings.common import filetools
from samplings.common.tools import (JsonEncoderX, TimeWatcher, logtime,
                                    now_time_millis, retry)
from samplings.db.base import Const, dingding_inconsistency

logger = logging.getLogger(__name__)
result_logger = logging.getLogger('result')

#################### main ####################

def main():
    # dbnames = Const.SRC_MONGO.list_database_names()
    # logger.info('dbnames: %s', dbnames)

    if Const.DATABASE not in Const.DEST_MONGO.list_database_names():
        logger.error("NO '%s' database in dest mongo!", Const.DATABASE)
        return

    db = Const.SRC_MONGO[Const.DATABASE]
    for col_name in db.list_collection_names():
        logger.info('db=%s, collection= %s', Const.DATABASE, col_name)
        if col_name not in Const.DEST_MONGO[Const.DATABASE].list_collection_names():
            logger.error("NO '%s' collection in dest mongo '%s' db!", col.name, col.database.name)
            continue

        col = db[col_name]
        task = build_task(col)
        if not task['finished']:
            col_cmp(col, task)

def build_task(col):
    # 1. 首先从文件反序列化得到表迭代任务信息
    file_path = Const.SAVE_DIR + col.name + Const.SUFFIX
    if os.path.exists(file_path):
        lines = filetools.read(file_path)
        return json.loads(lines[0])

    # 2. 从数据库查询表迭代任务信息
    count = aggregate_count(col)
    finished = False
    pk_type = None
    last_max = Const.MIN_STR
    if count == 0:
        logger.info("NO result in '%s' col!", col.name)
        finished = True
    else:
        result = col.find_one(projection={'createDate':False, 'updateDate':False})
        if isinstance(result['_id'], ObjectId):
            pk_type = Const.OBJECTID
            last_max = Const.MIN_OBJECTID_STR
        elif isinstance(result['_id'], str):
            pk_type = 'str'
        else:
            finished = True
            pk_type = str(type(result['_id']))
            logger.error('_id type (%s) is NOT ObjectId NOR str!', type(result['_id']))

    task = {
        'col_name': col.name,
        'pk_type': pk_type,
        'last_max': last_max,
        'count': count,
        'got_already': 0,
        'finished': finished,
        'create_time_millis': now_time_millis()
    }

    # save into file
    filetools.write(file_path, json.dumps(task))

    return task

def col_cmp(col, task):
    watcher = TimeWatcher()
    while True:
        watcher.reset()
        last_max = ObjectId(task['last_max']) if task['pk_type'] == Const.OBJECTID else task['last_max']
        this_min = last_max
        src_results = find_min_sort_limit(col, this_min, Const.STEPS)
        watcher.logtime('src find_min_sort_limit')

        src_dict = {}
        got_this_time = 0
        for result in src_results:
            got_this_time += 1
            last_max = result['_id']
            src_dict[result['_id']] = result
        watcher.logtime('src_results iterate')

        dest_col = Const.DEST_MONGO[col.database.name][col.name]
        dest_results = find_range(dest_col, this_min, last_max)
        watcher.logtime('dest find_range')

        dest_dict = {result['_id']: result for result in dest_results}
        watcher.logtime('dest_results iterate')

        dict_cmp(col.name, src_dict, dest_dict)
        watcher.logtime('dict_cmp')

        task['last_max'] = str(last_max)
        task['got_already'] += got_this_time
        logger.info('========== count=%s, got_this_time=%s, got_already=%s, last_max=%s',
            task['count'], got_this_time, task['got_already'], task['last_max'])

        if got_this_time < Const.STEPS:
            task['finished'] = True
            filetools.write(Const.SAVE_DIR + col.name + Const.SUFFIX, json.dumps(task))
            break
        else:
            filetools.write(Const.SAVE_DIR + col.name + Const.SUFFIX, json.dumps(task))

@retry(times=3)
# @logtime
def find_min_sort_limit(col, last_max_id, limit):
    # 'birthday': {'$gte':datetime.min, '$lte':datetime.max}
    return col.find({'_id':{'$gt':last_max_id}}, projection={'birthday':False})\
            .sort('_id', pymongo.ASCENDING)\
            .limit(limit)

@retry(times=3)
# @logtime
def find_range(col, this_min_id, last_max_id):
    return col.find(
                {'_id':{'$gt':this_min_id, '$lte':last_max_id}}, 
                projection={'birthday':False}
            )

def aggregate_count(col):
    '''
    use aggregate() to count the given collection accurately
    https://docs.mongodb.com/manual/reference/method/db.collection.count/#sharded-clusters
    '''
    results = col.aggregate([{'$group': {'_id': None, 'col_count': {'$sum': 1}}}])
    col_count = 0
    for result in results:
        col_count = result['col_count']
        logger.info('%s col_count=%s', col.name, col_count)
    
    return col_count

# @logtime
def dict_cmp(col_name, src_dict, dest_dict):
    errors = []
    for (id, src_result) in src_dict.items():
        if id not in dest_dict:
            # str(src_result) 避免数据库字段对应类转换为 json 异常
            error = _error_detail(col_name, Const.NO_ID_IN_DEST, id, str(src_result), None)
            errors.append(error)
            result_logger.info(json.dumps(error, ensure_ascii=False, cls=JsonEncoderX))
        else:
            smd5 = hashlib.md5(str(src_result).encode(encoding='utf8')).hexdigest()
            dmd5 = hashlib.md5(str(dest_dict[id]).encode(encoding='utf8')).hexdigest()
            if smd5 != dmd5:
                error = _error_detail(col_name, Const.MD5_NOT_EQUAL, id, str(src_result), str(dest_dict[id]))
                errors.append(error)
                result_logger.info(json.dumps(error, ensure_ascii=False, cls=JsonEncoderX))
    if errors:
        dingding_inconsistency('data inconsistency', errors)

def _error_detail(col_name, error, id, src, dest):
    return {
        'error': error,
        'db_type': Const.DB_TYPE,
        'database': Const.DATABASE,
        'col_name': col_name,
        '_id': id,
        'src': src,
        'dest': dest
    }

def aggregate_max_id(col):
    results = col.aggregate([{'$group': {'_id': None, 'max_id': {'$max': '$_id'}}}])
    max_id = None
    for result in results:
        max_id = result['max_id']
        print('collection=%s, max_id=%s' % (col.name, max_id))
    return max_id

def aggregate_min_id(col):
    results = col.aggregate([{'$group': {'_id': None, 'min_id': {'$min': '$_id'}}}])
    min_id = None
    for result in results:
        min_id = result['min_id']
        print('collection=%s, min_id=%s' % (col.name, min_id))
    return min_id


if __name__ == '__main__':
    main()
    # objectid_cmp_test()
