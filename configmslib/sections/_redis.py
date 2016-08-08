# encoding=utf8

""" The redis config section
    Author: lipixun
    Created Time : 一 12/ 7 10:43:58 2015

    File Name: _redis.py
    Description:

        The redis config section will not auto connect to redis server until a specific request when loaded

"""

from threading import Lock

from redis import StrictRedis

from configmslib.section import ReferConfigSection

class RedisConfigSection(ReferConfigSection):
    """The redis config section
    Known configs:
        - host                  (Required)The redis host
        - port                  The redis port
        - password              The redis password
        - timeout               The socket timeout in ms
    """
    Type = 'redis'
    ReloadRequired = True

    def validate(self, value):
        """Validate the config value
        """
        host = value.get("host")
        if not host:
            raise ValueError("Require host")

    def reference(self, config):
        """Get the referenced value from config
        """
        return RedisDatabase(config)

    def release(self, value):
        """Release the referenced mongodb client
        """
        value.close()

class RedisDatabase(object):
    """The redis database
    """
    DefaultPort            = 6379       # Default redis port
    DefaultTimeout         = 10         # 10s

    def __init__(self, config):
        """Create a new RedisDatabase
        """
        self.config = config
        self._lock = Lock()
        self._dbs = {}

    def __getitem__(self, db):
        """Get a database
        """
        redis = self._dbs.get(db)
        if not redis:
            with self._lock:
                redis = self._dbs.get(db)
                if not redis:
                    # Create new redis client
                    redis = self.createRedisByConfig(self.config, db)
                    self._dbs[db] = redis
        # Done
        return redis

    def close(self):
        """Close this database
        """
        for db, redis in self._dbs.iteritems():
            try:
                redis.disconnect()
            except:
                RedisConfigSection.logger.exception('[%s] Failed to disconnect redis for database [%s], ignore', RedisConfigSection.Type, db)

    @classmethod
    def createRedisByConfig(cls, config, db):
        """Create redis by config
        """
        host, port, password, timeout = \
                config['host'], \
                config.get('port', cls.DefaultPort), \
                config.get('password'), \
                config.get('timeout', cls.DefaultTimeout)
        RedisConfigSection.logger.info('[%s] Connecting to redis with host [%s] port [%s]', RedisConfigSection.Type, host, port)
        # Create the redis client
        return StrictRedis(host, port, db, password, timeout)
