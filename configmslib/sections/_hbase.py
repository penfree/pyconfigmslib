# encoding=utf8

""" The hbase config section
    Author: lipixun
    Created Time : 一 12/ 7 10:44:08 2015

    File Name: _hbase.py
    Description:

"""

import logging

import happybase

from ..section import ReferConfigSection

class HBaseConfigSection(ReferConfigSection):
    """The hbase config section
    Known configs:
        - host                  (Required)The hbase host
        - port                  The hbase port, 9090 by default
        - timeout               The timeout in ms
        - tablePrefix           The table prefix
        - tablePrefixSeparator  The table prefix separator
        - compat                The compatibility level
        - transport             The transport mode
        - protocol              The protocol mode
    """
    TYPE = 'hbase'

    DEFAULT_TIMEOUT = 10 * 1000         # 10s

    logger = logging.getLogger('config.hbase')

    def __init__(self, config):
        """Create a new HBaseConfigSection
        """
        # Super
        super(HBaseConfigSection, self).__init__(config)
        # Create the happybase connection
        self._conn = self.createConnectionByConfig(config)
        self._newConn = None

    def __getrefvalue__(self):
        """Get the referenced connection
        """
        return self._conn

    def __releaseref__(self):
        """Release the reference
        """
        if self._newConn:
            # Replace
            oldConn = self._conn
            self._conn = self._newConn
            self._newConn = None
            # Close old connection
            if oldConn:
                try:
                    oldConn.close()
                except:
                    self.logger.exception('Failed to close old hbase connection, ignore')

    def update(self, config):
        """Update the config
        """
        # Create new connection
        newConn = self.createConnectionByConfig(config)
        # Replace or not
        oldConn = None
        with self._lock:
            if self._refcount == 0:
                # Replace
                oldConn = self._conn
                self._conn = newConn
            else:
                # Set new connection and wait for de-reference
                self._newConn = newConn
        # Close old connection
        if oldConn:
            try:
                oldConn.close()
            except:
                self.logger.exception('Failed to close old hbase connection, ignore')
        # Super
        super(HBaseConfigSection, self).update(config)

    @classmethod
    def createConnectionByConfig(cls, config):
        """Create hbase connection by config
        """
        host, port, timeout, tablePrefix, tablePrefixSeparator, compat, transport, protocol = \
                config['host'], config.get('port'), config.get('timeout', cls.DEFAULT_TIMEOUT), config.get('tablePrefix'), \
                config.get('tablePrefixSeparator'), config.get('compat'), config.get('transport'), config.get('protocol')
        # Create the params
        params = { 'host': host }
        if port:
            params['port'] = port
        if timeout:
            params['timeout'] = timeout
        if tablePrefix:
            params['table_prefix'] = tablePrefix
        if tablePrefixSeparator:
            params['table_prefix_separator'] = tablePrefixSeparator
        if compat:
            params['compat'] = compat
        if transport:
            params['transport'] = transport
        if protocol:
            params['protocol'] = protocol
        # Create the connection
        return happybase.Connection(**params)

