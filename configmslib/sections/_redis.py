# encoding=utf8

""" The redis config section
    Author: lipixun
    Created Time : 一 12/ 7 10:43:58 2015

    File Name: _redis.py
    Description:

        The redis config section will not auto connect to redis server until a specific request when loaded

"""

import logging

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

    DEFAULT_PORT            = 6379      # Default redis port
    DEFAULT_TIMEOUT         = 10 * 1000 # 10s

    logger = logging.getLogger('config.redis')

    def __init__(self, config):
        """Create a new RedisConfigSection
        """
        # Super
        super(RedisConfigSection, self).__init__(config)
        # Create the redis database dict
        self._dbs = {}      # Key is db number, value is StrictRedis object
        self._requireUpdate = False

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

    def __releaseref__(self):
        """Release the referenced mongodb client
        """
        if self._requireUpdate:
            # Update the dbs
            oldDBs = self._dbs
            self._dbs = {}
            # Close all dbs
            for db, redis in oldDBs.iteritems():
                try:
                    redis.disconnect()
                except:
                    self.logger.exception('Failed to disconnect redis for database [%s]', db)
            # Done
            self._requireUpdate = False

    def update(self, config):
        """update the config
        """
        with self._lock:
            if self._refcount == 0:
                # Replase
                oldDBs = self._dbs
                self._dbs = {}
                for db, redis in oldDBs.iteritems():
                    try:
                        redis.disconnect()
                    except:
                        self.logger.exception('Failed to disconnect redis for database [%s]', db)
            else:
                # Update
                self._requireUpdate = True
        # Super
        super(RedisConfigSection, self).update(config)

    @classmethod
    def createRedisByConfig(cls, config, db):
        """Create redis by config
        """
        host, port, password, timeout = \
                config['host'], config.get('port', cls.DEFAULT_PORT), config.get('password'), config.get('timeout', cls.DEFAULT_TIMEOUT)
        if not timeout is None:
            timeout = timeout / 1000.0
        # Create the redis client
        return StrictRedis(host, port, db, password, timeout)

