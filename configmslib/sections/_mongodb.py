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

    def __init__(self, config):
        """Create a new MongodbConfigSection
        """
        # Super
        super(MongodbConfigSection, self).__init__(config)
        # Initialize the mongodb client
        self._client = self.createClientByConfig(config)
        self._newClient = None  # The new client
        # Set

    def __getrefvalue__(self):
        """Get the referenced mongodb client
        """
        return self._client

    def __releaseref__(self):
        """Release the referenced mongodb client
        """
        if self._newClient:
            # Replace
            oldClient = self._client
            self._client = self._newClient
            self._newClient = None
            # Close old client
            # NOTE:
            #   Here, we have to close the old client in the lock context
            #   We could do this in other thread in order to not block other reference request for closing old client, but this will require thread join and hard to do it right
            if oldClient:
                try:
                    oldClient.close()
                except:
                    self.logger.exception('Failed to close old mongodb client, ignore')

    def update(self, config):
        """Update the config
        """
        # Create new client
        newClient = self.createClientByConfig(config)
        # Replace or not
        oldClient = None
        with self._lock:
            if self._refcount == 0:
                # Replace
                oldClient = self._client
                self._client = newClient
            else:
                # Set new client and wait for de-reference
                self._newClient = newClient
        # Close oldClient
        if oldClient:
            try:
                oldClient.close()
            except:
                self.logger.exception('Failed to close old mongodb client, ignore')
        # Super
        super(MongodbConfigSection, self).update(config)

    @classmethod
    def createClientByConfig(cls, config):
        """Create a mongodb clieng by config
        """
        uri, timeout, connectTimeout, keepAlive = \
                config['uri'], config.get('timeout', cls.DEFAULT_TIMEOUT), config.get('connectTimeout', cls.DEFAULT_CONNECT_TIMEOUT), config.get('keepAlive', cls.DEFAULT_KEEP_ALIVE)
        # Create the client
        return MongoClient(host = uri, socketTimeoutMS = timeout, connectTimeoutMS = connectTimeout, socketKeepAlive = keepAlive)

