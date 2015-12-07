# encoding=utf8

""" The config sections
    Author: lipixun
    Created Time : 日 12/ 6 22:40:31 2015

    File Name: __init__.py
    Description:

"""

from _mongodb import MongodbConfigSection
from _redis import RedisConfigSection

SECTIONS = [
        MongodbConfigSection,
        RedisConfigSection
        ]

__all__ = [ 'SECTIONS', 'MongodbConfigSection', 'RedisConfigSection' ]

