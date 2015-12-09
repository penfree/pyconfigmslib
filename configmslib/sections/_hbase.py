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

    def __reference__(self, config):
        """Get the referenced connection
        """
        return self.createConnectionByConfig(config)

    def __release__(self, value):
        """Release the reference
        """
        value.close()

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
        cls.logger.info('Connecting to hbase with host [%s] port [%s]', host, port or '*')
        return happybase.Connection(**params)

