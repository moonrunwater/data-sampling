#!/usr/bin/python3
# -*- coding: UTF-8 -*-

import logging
import urllib

import mysql.connector
import psycopg2
import psycopg2.pool
import pymongo
from DBUtils.PersistentDB import PersistentDB

from samplings.common.tools import logtime, retry, dingding_robot

logger = logging.getLogger(__name__)

#################### const ####################

class Const:
    DB_TYPE = None
    DATABASE = None

    SRC_POOL = None 
    DEST_POOL = None

    SRC_MONGO = pymongo.MongoClient()
    DEST_MONGO = pymongo.MongoClient()

    STEPS = 10000
    MAX_STAGE = 100 * 10000

    SUFFIX = '.json'
    SAVE_DIR = None

    DINGTOKEN = 'f45a4ae936ce405c99a461f3a9fc26fa2ff0f714dba7e5e370b285a3b5affbed'

    # 字段/表名界定符 (防止特殊字符或与关键字冲突)
    DELIMITER = None
    MYSQL_DELIMITER = '`'
    PGSQL_DELIMITER = '"'

    MAX_THREAD = 1

    MIN_STR = ''
    MIN_NUM = -1
    OBJECTID = 'ObjectId'
    MIN_OBJECTID_STR = '000000000000000000000000'

    NO_ID_IN_SRC = 'NO_ID_IN_SRC'
    NO_ID_IN_DEST = 'NO_ID_IN_DEST'
    MD5_NOT_EQUAL = 'MD5_NOT_EQUAL'

#################### tool method ####################

# http://api.mongodb.com/python/current/api/pymongo/mongo_client.html
def mongo_pool(userpwd, hosts):
    return pymongo.MongoClient(
        "mongodb://%s:%s@%s" % (
            urllib.parse.quote_plus(userpwd.split(':')[0]),
            urllib.parse.quote_plus(userpwd.split(':')[1]),
            hosts
        ),
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
        retryWrites=False
    )

def mysql_pool(config, database):
    return PersistentDB(
        mysql.connector,
        database=database,
        maxusage=None, # unlimited reuse
        ping=1, # 1 = whenever it is requested
        **config)

def pgsql_pool(config, database):
    # return psycopg2.pool.ThreadedConnectionPool(3, 5, database=database, **config)
    # psycopg2.connect(database=database, **config)
    return PersistentDB(
        psycopg2,
        database=database,
        maxusage=None, # unlimited reuse
        ping=1, # 1 = whenever it is requested
        **config)

@retry(times=3)
@logtime
def query(pool, sql, *args):
    with pool.connection() as conn:
        cur = conn.cursor()
        # cur.execute("SELECT * FROM t1 WHERE id = %s", (5,))
        cur.execute(sql, args)
        return cur.fetchall()

def wrap(word):
    return '{delimiter}{word}{delimiter}'.format(word=word, delimiter=Const.DELIMITER)

# 钉钉提醒
def dingding_inconsistency(err_type, errors):
    if errors:
        content = {
            'type': err_type,
            'length': len(errors),
            'errors': errors[0: 100 if len(errors) > 100 else len(errors)]
        }
        dingding_robot(Const.DINGTOKEN, content)