# encoding=utf8

""" The elasticsearch config section
    Author: lipixun
    Created Time : 一 12/ 7 10:44:19 2015

    File Name: _elasticsearch.py
    Description:

"""

import logging

from elasticsearch import Elasticsearch
from elasticsearch.exceptions import TransportError

from configmslib.section import ReferConfigSection
from configmslib.util import json

esLogger = logging.getLogger('elasticsearch.trace')

DEFAULT_TIMEOUT = 30 * 1000     # 30s

class ElasticsearchConfigSection(ReferConfigSection):
    """The elasticsearch config section
    Known configs:
        - hosts                     (Required)A single host or a list of hosts
        - timeout                   The timeout in ms
    TODO:
        Add support to transport class and kwargs
    """
    TYPE = 'elasticsearch'

    logger = logging.getLogger('config.elasticsearch')

    def __reference__(self, config):
        """Get the referenced value
        """
        return self.createClientbyConfig(config)

    def __release__(self, value):
        """Release the value
        """
        pass

    def __withinerror__(self, error):
        """When error occurred in the with statements
        """
        if isinstance(error, TransportError):
            # Print the log
            self.logger.error('Failed to invoke elasitcsearch, error code [%s] message [%s] info [%s]' % (
                error.status_code,
                error.error,
                json.dumps(error.info, ensure_ascii = False) if error.info else ''
                ))

    @classmethod
    def createClientbyConfig(cls, config):
        """Create elasticsearch client by config
        """
        hosts = config['hosts']
        if isinstance(hosts, basestring):
            hosts = (hosts, )
        timeout = config.get('timeout', DEFAULT_TIMEOUT)
        cls.logger.info('Connecting to elasticsearch with hosts: %s timeout [%s]', hosts, timeout)
        # Create the client
        return Elasticsearch(hosts, timeout = float(timeout) / 1000.0)
