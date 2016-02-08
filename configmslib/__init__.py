# encoding=utf8

""" The config management system client lib
    Author: lipixun
    Created Time : 日 12/ 6 22:38:44 2015

    File Name: __init__.py
    Description:

"""

from __version__ import __version__

from repository import KNOWN_SECTION_TYPES, ConfigRepository, NormalName, PrefixName

__all__ = [ 'KNOWN_SECTION_TYPES', 'ConfigRepository', 'NormalName', 'PrefixName' ]

