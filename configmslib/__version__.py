# encoding=utf8

""" The version
    Author: lipixun
    Created Time : 日 12/ 6 22:39:05 2015

    File Name: __version__.py
    Description:

"""

# The version, in format major.minor
__version__ = '0.1'

import os.path

versionFile = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'VERSION')
if os.path.isfile(versionFile):
    # Use the version in file
    with open(versionFile, 'rb') as fd:
        __version__ = fd.read()

def setVersion(version):
    """Set the version
    """
    with open(versionFile, 'wb') as fd:
        print >>fd, version,


