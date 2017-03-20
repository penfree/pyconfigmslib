#!/usr/bin/env python
# coding=utf-8
'''
Author: qiupengfei@iyoudoctor.com

'''
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

from configmslib.section import ReferConfigSection

class HDFSConfigSection(ReferConfigSection):
    '''
        Known configs:
            hosts: # namenode hosts
                - host: namenode1.domain
                  port: 8020 (default)
                - host: namenode2.domain
                  port: 8020 (default)
            use_trash: false (default)
            user: hdfs (default)
            timeout: 30 (default, in seconds)
    '''
    Type = 'hdfs'
    ReloadRequired = True
    DefaultPort = 8020
    DefaultUseTrash = False
    DefaultUser = 'hdfs'
    DefaultTimeout = 30

    def validate(self, value):
        if not value.get('hosts'):
            raise ValueError('hosts required')
        else:
            for host in value.get('hosts'):
                if not value.get('host'):
                    raise ValueError('host required')
    
    def reference(self, config):
        '''
            Create hdfs connection
        '''
        from snakebite.client import HAClient
        from snakebite.client import Namenode

        use_trash = config.get('use_trash', False)
        user = config.get('user', self.DefaultUser)
        timeout = config.get('timeout', self.DefaultTimeout)

        namenodes = [
            Namenode(host['host'], host.get('port', self.DefaultPort)) for host in config['hosts']
        ]

        client = HAClient(namenodes, use_trash, effective_user=user, sock_request_timeout=timeout)

        return client

    def release(self, value):
        '''
            release connection
        '''
        # Nothing to do
        pass