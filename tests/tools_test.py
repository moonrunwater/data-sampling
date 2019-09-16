#!/usr/bin/python3
# -*- coding: utf-8 -*-

import functools
import logging
import time

from samplings.common.tools import (TimeWatcher, dingding_robot, logtime,
                                    now_time_millis, retry)


logger = logging.getLogger(__name__)


def test_dingding_robot():
    dingding_robot('f45a4ae936ce405c99a461f3a9fc26fa2ff0f714dba7e5e370b285a3b5affbed', '火狐测试')
    dct = {
        'name': '火狐',
        '国家': '中国',
        'people': 500
    }
    dingding_robot('f45a4ae936ce405c99a461f3a9fc26fa2ff0f714dba7e5e370b285a3b5affbed', dct)


def test_time_watcher():
    watcher = TimeWatcher()
    time.sleep(1)
    watcher.logtime('sleep 1s')
    time.sleep(2)
    watcher.logtime('sleep 2s')
    time.sleep(3)
    watcher.logtime('sleep 3s')

    time.sleep(1)

    watcher.reset()
    time.sleep(1)
    watcher.logtime('sleep 1s, reset, sleep 1s')


def test_now_time_millis():
    start = now_time_millis()
    logger.debug('start: %s', start)

    time.sleep(5)

    end = now_time_millis()
    logger.debug('end: %s', end)
    logger.debug('execute: %sms', end - start)


@logtime
def test_logtime_wrap(name, age):
    logger.info('name=%s, age=%s', name, age)


@retry(times=3)
def test_retry_wrap(company, money):
    logger.info('company=%s, money=%s', company, money)
    raise Exception('test_retry_wrap error')


@retry(times=5)
@logtime
def test_retry_logtime_wrap(num, count):
    logger.info('num=%s, count=%s', num, count)
    raise Exception('test_retry_logtime_wrap error')


# project root ddirectory 执行 python -m tests.tools_test
# ~/github/moonrunwater/data-sampling$ python -m tests.tools_test
if __name__ == "__main__":
    # test_time_watcher()
    # test_now_time_millis()
    # test_logtime_wrap('huohu', 99)
    # test_retry_wrap('hlj', 9999999999.99)
    # test_retry_logtime_wrap(1, 9999)
    test_dingding_robot()
