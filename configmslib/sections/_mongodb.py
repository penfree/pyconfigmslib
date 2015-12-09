# encoding=utf8

""" The mongodb config section
    Author: lipixun
    Created Time : 一 12/ 7 10:43:50 2015

    File Name: _mongodb.py
    Description:

        The mongodb config section will auto connect to mongodb when loaded

"""

import logging

from pymongo import MongoClient

from ..section import ReferConfigSection

class MongodbConfigSection(ReferConfigSection):
    """The mongodb config section
    Known configs:
        - uri               (Required)The connection uri
        - timeout           The socket timeout in ms
        - connectTimeout    The socket connect timeout in ms
        - keepAlive         Enable keep alive or not
    """
    TYPE = 'mongodb'

    DEFAULT_TIMEOUT         = 10 * 1000     # 10s
    DEFAULT_CONNECT_TIMEOUT = 10 * 1000     # 10s
    DEFAULT_KEEP_ALIVE      = False         # Do not keep alive by default

    logger = logging.getLogger('config.mongodb')

    def __reference__(self, config):
        """Get the referenced mongodb client
        """
        return self.createClientByConfig(config)

    def __release__(self, value):
        """Release the referenced mongodb client
        """
        value.close()

    @classmethod
    def createClientByConfig(cls, config):
        """Create a mongodb clieng by config
        """
        uri, timeout, connectTimeout, keepAlive = \
                config['uri'], config.get('timeout', cls.DEFAULT_TIMEOUT), config.get('connectTimeout', cls.DEFAULT_CONNECT_TIMEOUT), config.get('keepAlive', cls.DEFAULT_KEEP_ALIVE)
        cls.logger.info('Connecting to mongodb with uri [%s]', uri)
        # Create the client
        return MongoClient(host = uri, socketTimeoutMS = timeout, connectTimeoutMS = connectTimeout, socketKeepAlive = keepAlive)

