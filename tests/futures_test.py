#!/usr/bin/python3
# -*- coding: utf-8 -*-

import logging
import time
from concurrent import futures


logger = logging.getLogger(__name__)


def sleep_demo(num):
    logger.info(num)
    time.sleep(num % 5)
    return num % 5

def test_thread_pool():
    with futures.ThreadPoolExecutor(max_workers=20, thread_name_prefix='thread_pool') as executor:
        future_list = []
        for i in range(1, 100):
            future = executor.submit(sleep_demo, i)
            future_list.append(future)

        # 阻塞主线程, 直到线程池里面的所有任务都完成 ALL_COMPLETED
        futures.wait(future_list, return_when=futures.ALL_COMPLETED)
        for future in future_list:
            rt = future.result()
            logger.info(rt)


# project root ddirectory 执行 python -m tests.futures_test
# ~/github/moonrunwater/data-sampling$ python -m tests.futures_test
if __name__ == "__main__":
    test_thread_pool()

