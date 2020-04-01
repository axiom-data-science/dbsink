#!python
# coding=utf-8
import logging


log_format = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stream = logging.StreamHandler()
stream.setFormatter(log_format)

ea = logging.getLogger('easyavro')
ea.setLevel(logging.INFO)
ea.addHandler(stream)

L = logging.getLogger()
L.setLevel(logging.INFO)
L.handlers = [stream]

__version__ = "2.5.0"
