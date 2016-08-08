# encoding=utf8

""" The hbase config section
    Author: lipixun
    Created Time : 一 12/ 7 10:44:08 2015

    File Name: _hbase.py
    Description:

"""

import happybase

from configmslib.section import ReferConfigSection

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
    Type = "hbase"
    ReloadRequired = True
    DefaultTimeout = 10.0       # 10s
    DefaultPoolSize = 3

    def validate(self, value):
        """Validate the config value
        """
        host = value.get("host")
        if not host:
            raise ValueError("Require host")

    def __reference__(self, config):
        """Get the referenced connection
        """
        host, port, timeout, tablePrefix, tablePrefixSeparator, compat, transport, protocol, poolSize = \
                config["host"], \
                config.get("port", 9200), \
                config.get("timeout", self.DefaultTimeout), \
                config.get("tablePrefix"), \
                config.get("tablePrefixSeparator"), \
                config.get("compat"), \
                config.get("transport"), \
                config.get("protocol"), \
                config.get("poolSize", self.DefaultPoolSize)
        # Create the params
        params = { "host": host }
        if port:
            params["port"] = port
        if timeout:
            params["timeout"] = timeout
        if tablePrefix:
            params["table_prefix"] = tablePrefix
        if tablePrefixSeparator:
            params["table_prefix_separator"] = tablePrefixSeparator
        if compat:
            params["compat"] = compat
        if transport:
            params["transport"] = transport
        if protocol:
            params["protocol"] = protocol
        if not poolSize:
            params["size"] = poolSize
        self.logger.info("[%s] Connecting to hbase with host [%s] port [%s]", self.Type, host, port)
        # Create the connection pool
        return happybase.ConnectionPool(**params)

    def reloase(self, value):
        """Release the reference
        """
        # Close all the connections in the pool
        succ, failed = 0, 0
        while not value._queue.empty():
            conn = value._queue.get(False)
            try:
                if conn:
                    conn.close()
                succ += 1
            except:
                self.logger.exception("[%s] Failed to close hbase connection", self.Type)
                failed += 1
        # Done
        self.logger.info('[%s] Release pool: [%d] hbase connection closed successfully and [%d] failed' % (self.Type, succ, failed))

    def __getinstancevalue__(self, value):
        """Get the value returned by instance method
        """
        with value.value.connection(timeout = self.get("timeout", self.DefaultTimeout)) as connection:
            yield connection
