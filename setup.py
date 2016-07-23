# encoding=utf8

"""
    Author: lipixun
    Created Time : 四 12/17 17:28:50 2015

    File Name: setup.py
    Description:

"""

import sys
reload(sys)
sys.setdefaultencoding('utf8')

from setuptools import setup, find_packages

requirements = [ x.strip() for x in open('requirements.txt').readlines() ]

setup(
    name = 'configmslib',
    author = 'lipixun',
    author_email = 'lipixun@outlook.com',
    url = 'https://github.com/lipixun/pyconfigmslib',
    packages = find_packages(),
    install_requires = requirements,
    description = 'The config lib',
    long_description = open('README.md').read(),
)

