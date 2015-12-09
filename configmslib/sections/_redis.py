# encoding=utf8

""" The redis config section
    Author: lipixun
    Created Time : 一 12/ 7 10:43:58 2015

    File Name: _redis.py
    Description:

        The redis config section will not auto connect to redis server until a specific request when loaded

"""

import logging

from threading import Lock

from redis import StrictRedis

from ..section import ReferConfigSection

class RedisConfigSection(ReferConfigSection):
    """The redis config section
    Known configs:
        - host                  (Required)The redis host
        - port                  The redis port
        - password              The redis password
        - timeout               The socket timeout in ms
    """
    TYPE = 'redis'

    logger = logging.getLogger('config.redis')

    def __reference__(self, config):
        """Get the referenced value from config
        """
        return RedisDatabase(config)

    def __release__(self, value):
        """Release the referenced mongodb client
        """
        value.close()

class RedisDatabase(object):
    """The redis database
    """
    DEFAULT_PORT            = 6379      # Default redis port
    DEFAULT_TIMEOUT         = 10 * 1000 # 10s

    logger = logging.getLogger('config.redis.database')

    def __init__(self, config):
        """Create a new RedisDatabase
        """
        self.config = config
        self._lock = Lock()
        self._dbs = {}

    def __getitem__(self, db):
        """Get a database
        """
        with self._lock:
            redis = self._dbs.get(db)
            if not redis:
                # Create new redis client
                redis = self.createRedisByConfig(self.config, db)
                self._dbs[db] = redis
            return redis

    def close(self):
        """Close this database
        """
        for db, redis in self._dbs.iteritems():
            try:
                redis.disconnect()
            except:
                self.logger.exception('Failed to disconnect redis for database [%s], ignore', db)

    @classmethod
    def createRedisByConfig(cls, config, db):
        """Create redis by config
        """
        host, port, password, timeout = \
                config['host'], config.get('port', cls.DEFAULT_PORT), config.get('password'), config.get('timeout', cls.DEFAULT_TIMEOUT)
        if not timeout is None:
            timeout = timeout / 1000.0
        cls.logger.info('Connecting to redis with host [%s] port [%s]', host, port)
        # Create the redis client
        return StrictRedis(host, port, db, password, timeout)

