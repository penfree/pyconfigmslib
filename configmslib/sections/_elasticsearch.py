# encoding=utf8

""" The elasticsearch config section
    Author: lipixun
    Created Time : 一 12/ 7 10:44:19 2015

    File Name: _elasticsearch.py
    Description:

"""

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import TransportError

from configmslib.section import ReferConfigSection
from configmslib.util import json

class ElasticsearchConfigSection(ReferConfigSection):
    """The elasticsearch config section
    Known configs:
        - hosts                     (Required)A single host or a list of hosts
        - timeout                   The timeout in ms
    TODO:
        Add support to transport class and kwargs
    """
    Type = "elasticsearch"
    ReloadRequired = True
    DefaultTimeout = 30.0   # 30s

    def validate(self, value):
        """Validate the config value
        """
        hosts = value.get("hosts")
        if not isinstance(hosts, (basestring, list, tuple)):
            raise ValueError("Invalid hosts")

    def reference(self, config):
        """Get the referenced value
        """
        hosts = config.get("hosts")
        if isinstance(hosts, basestring):
            hosts = (hosts, )
        timeout = config.get("timeout", self.DefaultTimeout)
        # Log it
        self.logger.info("[%s] Connecting to elasticsearch [%s] timeout [%s]", self.Type, ",".join(hosts), timeout)
        # Create the client
        return Elasticsearch(hosts, timeout = float(timeout))

    def withinError(self, error):
        """When error occurred in the with statements
        """
        if isinstance(error, TransportError):
            # Print the log
            self.logger.error('Failed to invoke elasitcsearch, error code [%s] message [%s] info [%s]' % (
                error.status_code,
                error.error,
                json.dumps(error.info, ensure_ascii = False) if error.info else ''
                ))
