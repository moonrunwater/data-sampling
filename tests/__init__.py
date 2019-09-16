#!/usr/bin/python3
# -*- coding: utf-8 -*-


#################### sys.path 工程根目录 ####################

import sys
import os

sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))
# print(sys.path)


#################### 初始化操作 ####################

import samplings.log
