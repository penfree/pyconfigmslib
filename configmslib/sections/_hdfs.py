#!/usr/bin/env python
# coding=utf-8
'''
Author: qiupengfei@iyoudoctor.com

'''
import sys
reload(sys)
sys.setdefaultencoding('utf-8')

from configmslib.section import ReferConfigSection
import os

class HDFSConfigSection(ReferConfigSection):
    '''
        Known configs:
            hosts: # namenode hosts
                - host: namenode1.domain
                  port: 8020 (default)
                - host: namenode2.domain
                  port: 8020 (default)
            user: hdfs (default)
    '''
    Type = 'hdfs'
    ReloadRequired = True
    DefaultPort = 8020
    DefaultUser = 'hdfs'

    def validate(self, value):
        if not value.get('hosts'):
            raise ValueError('hosts required')
        else:
            for host in value.get('hosts'):
                if not host.get('host'):
                    raise ValueError('host required')

    def reference(self, config):
        '''
            Create hdfs connection
        '''
        host = ''
        port = None
        pars = {}
        if os.environ.get('USE_KERBEROS') == '1':
            pars['hadoop.security.authentication'] = 'kerberos'
        if len(config.get('hosts')) > 1:
            # HA mode
            host = 'nn'
            pars['dfs.nameservices'] = 'nn'
            namenodes = []
            for idx, item in enumerate(config['hosts'], start=1):
                namenodes.append('nn%d' % idx)
                pars['dfs.namenode.rpc-address.nn.nn%d' % idx] = "{host}:{port}".format(host=item['host'], port=item.get('port', self.DefaultPort))
            pars['dfs.ha.namenodes.nn'] = ','.join(namenodes)
        else:
            # Single namenode mode
            host = config['hosts'][0]['host']
            port = config['hosts'][0].get('port', self.DefaultPort)

        import hdfs3
        fs = hdfs3.HDFileSystem(host=host, port=port, pars=pars, user=config.get('user', self.DefaultUser))
        return fs

    def release(self, value):
        '''
            release connection
        '''
        # Nothing to do
        value.disconnect()
