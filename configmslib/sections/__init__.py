# encoding=utf8

""" The config sections
    Author: lipixun
    Created Time : 日 12/ 6 22:40:31 2015

    File Name: __init__.py
    Description:

"""

from _mongodb import MongodbConfigSection
from _redis import RedisConfigSection
from _hbase import HBaseConfigSection
from _elasticsearch import ElasticsearchConfigSection
from _dict import DictConfigSection
from _hdfs import HDFSConfigSection

SECTIONS = [
        MongodbConfigSection,
        RedisConfigSection,
        HBaseConfigSection,
        ElasticsearchConfigSection,
        DictConfigSection,
        HDFSConfigSection
        ]

__all__ = [ 'SECTIONS' ]