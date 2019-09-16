#!/usr/bin/python3
# -*- coding: utf-8 -*-

import functools
import logging
import time

from samplings.common import filetools


logger = logging.getLogger(__name__)


def test_write():
    filetools.write('test.txt', 'Hello, 这是一段测试文字!')


# project root ddirectory 执行 python -m tests.filetools_test
# ~/github/moonrunwater/data-sampling$ python -m tests.filetools_test
if __name__ == "__main__":
    test_write()
