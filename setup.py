# encoding=utf8

""" The configmslib setup script
    Author: lipixun
    Created Time : 三  1/20 20:11:05 2016

    File Name: setup.py
    Description:

"""

import sys
reload(sys)
sys.setdefaultencoding('utf8')

from datetime import datetime

import configmslib

from setuptools import setup, find_packages

requirements = [ x.strip() for x in open('requirements.txt').readlines() ]

# Fix up the version
version = configmslib.__version__
if len(version.split('.')) < 3:
    version = '%s.%s' % (version, datetime.now().strftime('%s'))
    configmslib.setVersion(version)

setup(
    name = 'pyconfigmslib',
    version = version,
    author = 'lipixun',
    author_email = 'lipixun@iyoudoctor.com',
    url = 'https://github.com/lipixun/pyconfigmslib',
    packages = find_packages(),
    install_requires = requirements,
    description = 'The config management system lib',
    long_description = open('README.md').read(),
)

