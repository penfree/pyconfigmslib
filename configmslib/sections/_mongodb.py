# encoding=utf8

""" The mongodb config section
    Author: lipixun
    Created Time : 一 12/ 7 10:43:50 2015

    File Name: _mongodb.py
    Description:

        The mongodb config section will auto connect to mongodb when loaded

"""

from pymongo import MongoClient

from configmslib.section import ReferConfigSection

class MongodbConfigSection(ReferConfigSection):
    """The mongodb config section
    Known configs:
        - uri               (Required)The connection uri
        - timeout           The socket timeout in ms
        - connectTimeout    The socket connect timeout in ms
        - keepAlive         Enable keep alive or not
    """
    Type = 'mongodb'
    ReloadRequired = True
    DefaultTimeout = 10                 # 10s
    DefaultConnectionTimeout = 10       # 10s
    DefaultKeepAlive = False            # Do not keep alive by default

    def validate(self, value):
        """Validate the config value
        """
        uri = value.get("uri")
        if not uri:
            raise ValueError("Require uri")

    def reference(self, config):
        """Get the referenced mongodb client
        """
        uri, timeout, connectTimeout, keepAlive, replicaSetName = \
                config['uri'], \
                config.get('timeout', self.DefaultTimeout), \
                config.get('connectTimeout', self.DefaultConnectionTimeout), \
                config.get('keepAlive', self.DefaultKeepAlive), \
                config.get('replicaSet'),
        self.logger.info('[%s] Connecting to mongodb with uri [%s] replicaSet [%s]', self.Type, uri, replicaSetName)
        # Create the client
        return MongoClient(
            host = uri,
            socketTimeoutMS = timeout * 1000.0,
            connectTimeoutMS = connectTimeout * 1000.0,
            socketKeepAlive = keepAlive,
            replicaset = replicaSetName,
        )

    def release(self, value):
        """Release the referenced mongodb client
        """
        value.close()
