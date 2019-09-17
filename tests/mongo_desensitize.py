#!/usr/bin/env python
# -*-coding:utf-8-*-

import argparse

import pymongo
from bson.objectid import ObjectId

MIN_OBJECTID_STR = '000000000000000000000000'
STEPS = 1000

# http://api.mongodb.com/python/current/api/pymongo/mongo_client.html
def mongo_pool(hosts, auth_source, username, password):
    return pymongo.MongoClient('mongodb://' + hosts,
                               authSource=auth_source,
                               username=username,
                               password=password,
                               connect=True,
                               maxPoolSize=10,
                               minPoolSize=10,
                               maxIdleTimeMS=30 * 1000,
                               # wait for a response after sending an ordinary (non-monitoring) database operation
                               # before concluding that a network error has occurred
                               socketTimeoutMS=60 * 1000,
                               # wait during server monitoring when connecting a new socket to a server
                               # before concluding the server is unavailable
                               connectTimeoutMS=5 * 1000,
                               serverSelectionTimeoutMS=30 * 1000,
                               waitQueueTimeoutMS=10 * 1000,
                               waitQueueMultiple=5,
                               heartbeatFrequencyMS=10 * 1000,
                               appname="mongo_sampling",
                               retryWrites=False)


def desensitize_col(col):
    last_max_id = ObjectId(MIN_OBJECTID_STR)
    while True:
        docs = find_min_sort_limit(col, last_max_id, STEPS)

        got_this_time = 0
        update_count = 0
        for doc in docs:
            got_this_time += 1
            last_max_id = doc['_id']
            # print(doc)
            update_count += desensitize_document(col, doc)
            # rollback_document(col, doc)

        print('== got_this_time:', got_this_time, 'update_count:', update_count)
        if got_this_time < STEPS:
            break


def find_min_sort_limit(col, last_max_id, limit):
    return col.find({'_id': {'$gt': last_max_id}})\
        .sort('_id', pymongo.ASCENDING)\
        .limit(limit)


def desensitize_document(col, doc):
    if len(doc['userid']) == 32:
        result = col.update_one({'_id': doc['_id']}, {
                                '$set': {'userid': '9' + doc['userid']}})
        # print(doc['userid'], result.matched_count, result.modified_count)
        return 1
    else:
        # print(len(doc['userid']), doc['userid'][0])
        return 0


def rollback_document(col, doc):
    if len(doc['userid']) == 33:
        result = col.update_one({'_id': doc['_id']}, {
                                '$set': {'userid': doc['userid'][1:]}})
        # print(doc['userid'], result.matched_count, result.modified_count)
        return 1
    else:
        # print(len(doc['userid']), doc['userid'][0])
        return 0


# 支持 python2 及 python3
# 依赖
# pip install pymongo
# 执行
# nohup \
# python mongo_desensitize.py \
#     --hosts=xx1.com:20000,xx2.com:20000,xx3.com:20000 \
#     --authdb=*** \
#     --username=***_user \
#     --password=***_pwd \
#     --db=*** \
#     --steps=1000 \
# > log 2>&1 &

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("-s", "--hosts", required=True, type=str, help="mongo host: host1:port1,host2:port2,host3:port3")
    parser.add_argument("-a", "--authdb", required=True, type=str, help="auth db")
    parser.add_argument("-u", "--username", required=True, type=str, help="username")
    parser.add_argument("-p", "--password", required=True, type=str, help="password")
    parser.add_argument("-d", "--db", required=True, type=str, help="db")
    parser.add_argument("--steps", required=False, type=int, default=1000, help="query steps, default=1000")
    args = parser.parse_args()

    STEPS = args.steps

    mongo = mongo_pool(args.hosts, args.authdb, args.username, args.password)
    db = mongo[args.db]

    for col_name in db.list_collection_names():
        if col_name == 'system.profile':
            continue

        print('\n==========', col_name)
        col = db[col_name]
        desensitize_col(col)
