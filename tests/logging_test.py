#!/usr/bin/python3
# -*- coding: UTF-8 -*-


import logging
import time


def log_test():
    result = logging.getLogger("result")
    result.info("This is result logger test!")

    log = logging.getLogger(__name__)
    log.debug("This is root logger debug test!")
    log.info("This is root logger info test!")
    log.error("This is root logger error test!")

    log = logging.getLogger()
    log.debug("This is root logger debug test!")
    log.info("This is root logger info test!")
    log.error("This is root logger error test!")


# project root ddirectory 执行 python -m tests.logging_test
# ~/github/moonrunwater/data-sampling$ python -m tests.logging_test
if __name__ == "__main__":
    log_test()
