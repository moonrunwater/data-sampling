#!/usr/bin/python3
# -*- coding: utf-8 -*-

import json
import logging
import logging.config
import os
from io import StringIO


# log_dir = "/tmp/python/logs"
log_dir = "./logs"
if not os.path.exists(log_dir):
    os.makedirs(log_dir)

# print(os.path.dirname(os.path.relpath(__file__)))
here_dir = os.path.dirname(os.path.abspath(__file__))
logging_path = here_dir + '/logging.'
try:
    import yaml
except ImportError as e:
    logging_path += 'json'
else:
    logging_path += 'yaml'

temp_sio = StringIO()
config = None
if os.path.exists(logging_path):
    with open(logging_path, 'r', encoding='utf-8') as f:
        temp_sio.writelines(f.readlines())
        temp_sio.flush()
        if logging_path.endswith('yaml'):
            config = yaml.safe_load(StringIO(temp_sio.getvalue().format(log_dir=log_dir)))
        elif logging_path.endswith('json'):
            config = json.load(StringIO(temp_sio.getvalue().replace('$$LOG_DIR$$', log_dir)))

if config:
    logging.config.dictConfig(config)
else:
    logging.basicConfig(level=logging.INFO)
    logging.getLogger().warning("NO logging config file -> USE basic logging config.")
