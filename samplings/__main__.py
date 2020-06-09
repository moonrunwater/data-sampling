#!/usr/bin/python3
# -*- coding: utf-8 -*-

import argparse
import logging
import os
import sys
import traceback

import samplings.db
from samplings.common import tools
from samplings.db import base, mongo, mysql, pgsql
from samplings.db.base import Const

logger = logging.getLogger(__name__)

def main():
    # init params
    _init()

    # restart if not done, max times = 20
    done = False
    restart_times = 0
    while not done and restart_times <= 20:
        done = _main(restart_times)
        restart_times += 1

    # 钉钉通知
    content = {
        'type': 'info',
        'done': done,
        'restart_times': restart_times,
        'dbtype': Const.DB_TYPE,
        'database': Const.DATABASE
    }
    tools.dingding_robot(Const.DINGTOKEN, content)

def _main(restart_times):
    try:
        if Const.DB_TYPE == 'mysql':
            mysql.MysqlSampling().main()
        elif Const.DB_TYPE == 'pgsql':
            pgsql.PgsqlSampling().main()
        elif Const.DB_TYPE == 'mongo':
            mongo.main()
    except Exception as e:
        # logger.exception <=> logger.error(msg, exc_info=exc_info), 会输出 traceback 信息
        logger.exception(repr(e))
        # 钉钉提醒
        content = {
            'type': 'error',
            'exception': repr(e),
            'restart_times': restart_times,
            # 'traceback': traceback.format_exc()
        }
        tools.dingding_robot(Const.DINGTOKEN, content)
        return False
    else:
        return True

def _init():
    parser = argparse.ArgumentParser()

    parser.add_argument("-t", "--dbtype", required=True, type=str, choices=['mysql', 'pgsql', 'mongo'],
                        help="db type: mysql or pgsql or mongo")
    parser.add_argument("-d", "--database", required=True, type=str, help="database")
    parser.add_argument("-dd", "--destdatabase", required=True, type=str, help="dest database")
    parser.add_argument("-sh", "--srchostport", required=True, type=str, help="source host:port")
    parser.add_argument("-su", "--srcuserpwd", required=True, type=str, help="source user:password")
    parser.add_argument("-dh", "--desthostport", required=True, type=str, help="destination user:password")
    parser.add_argument("-du", "--destuserpwd", required=True, type=str, help="destination user:password")
    parser.add_argument("--steps", required=False, type=int, default=10000, help="query steps, default=10000")
    parser.add_argument("-sp", "--savepath", required=False, type=str, default='/tmp/python/samplings', help="save path")

    args = parser.parse_args()

    Const.DB_TYPE = args.dbtype.lower()
    Const.DATABASE = args.database.lower()
    Const.DEST_DATABASE = args.destdatabase.lower()

    Const.SAVE_DIR = args.savepath + '/' + Const.DB_TYPE + '/' +  Const.DATABASE + '/'
    if not os.path.exists(Const.SAVE_DIR):
        os.makedirs(Const.SAVE_DIR)

    Const.STEPS = args.steps
    logger.info('DB_TYPE=%s, DATABASE=%s, SAVE_DIR=%s, STEPS=%s', Const.DB_TYPE, Const.DATABASE, Const.SAVE_DIR, Const.STEPS)

    if Const.DB_TYPE in ['mysql', 'pgsql']:
        # user:password
        # host:port
        src_config = tools.build_config(args.srchostport, args.srcuserpwd)
        dest_config = tools.build_config(args.desthostport, args.destuserpwd)

        if Const.DB_TYPE == 'mysql':
            Const.DELIMITER = Const.MYSQL_DELIMITER
            Const.SRC_POOL = base.mysql_pool(src_config, Const.DATABASE)
            Const.DEST_POOL = base.mysql_pool(dest_config, Const.DEST_DATABASE)
        else:
            Const.DELIMITER = Const.PGSQL_DELIMITER
            Const.SRC_POOL = base.pgsql_pool(src_config, Const.DATABASE)
            Const.DEST_POOL = base.pgsql_pool(dest_config, Const.DEST_DATABASE)
    elif Const.DB_TYPE == 'mongo':
        Const.SRC_MONGO = base.mongo_pool(args.srcuserpwd, args.srchostport)
        Const.DEST_MONGO = base.mongo_pool(args.destuserpwd, args.desthostport)


if __name__ == '__main__':
    main()
