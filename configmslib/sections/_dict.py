#!/usr/bin/env python
#coding=utf8
"""
# Author: f
# Created Time : 六 11/ 5 12:11:32 2016

# File Name: _dict.py
# Description:

"""

import sys
reload(sys)
sys.setdefaultencoding('utf-8')

from configmslib.section import ReferConfigSection
from threading import Lock
import gridfs
import simplejson as json
import logging
import os

LOG = logging.getLogger("configms.sections.dict")

class DictConfigSection(ReferConfigSection):
    """DictClientConfigSection
        Configs:
            - dbtype: gridfs | mongodb | elasticsearch
                - For elasticsearch:
                    - index: the index name
                    - doctype: the doc type
                - For mongodb and gridfs:
                    - database: database name
                    - collection: collection name, default to 'fs' for gridfs
                - For gridfs:
                    - filename
            - backend: the backend database where data is stored, should be in the same repository
            - datatype: json | kv,  when dbtype is mongodb or elasticsearch, type is allways json, when dbtype is gridfs, both json and kv can be set
                - For json:
                    - key_field: list of key field
                    - value_field: the value field, default to entire obj
                - For kv: first column is key, second column is value, all the other columns is ignored
            - cache_path: the dict will be cached to disk to avoid downloading everytime, only used when debug locally
    """
    Type = 'dict'
    ReloadRequired = True

    def validate(self, value):
        if not value.get('dbtype'):
            raise ValueError('dbtype is required')
        if value.get('dbtype') == 'elasticsearch':
            if not value.get('index') or not value.get('doctype'):
                raise ValueError('index and doctype is required')
        elif value.get('dbtype') == 'mongodb':
            if not value.get('database') or not value.get('collection'):
                raise ValueError('database and collection is required')
        elif value.get('dbtype') == 'gridfs':
            if not value.get('database') or not value.get('filename'):
                raise ValueError('database and filename is required')
        else:
            raise ValueError('unknown dbtype')

    def reference(self, config):
        """
            @Brief reference
            @Param config:
        """
        if hasattr(self, '_value') and self._value:
            return self._value
        else:
            return DictObj.getDict(self.key, config, self.repository)

    def release(self, value):
       """
           @Brief release
           @Param value:
       """
       pass
       

class DictObj(dict):
    def __init__(self, name, config, repository):
        self.name = name
        self.config = config
        self.repository = repository
        self.dbtype = config.get('dbtype')
        self.backend = config['backend']
        if self.dbtype == 'mongodb' or self.dbtype == 'elasticsearch':
            self.datatype = 'json'
        else:
            self.datatype = config.get('datatype', 'json')
        self.cache_path = config.get('cache_path')
        self._lock = Lock()

    @classmethod
    def getDict(self, name, config, repository):
        if config['dbtype'] == 'elasticsearch':
            handler = ElasticDict(name, config, repository)
        elif config['dbtype'] == 'mongodb':
            handler = MongoDict(name, config, repository)
        elif config['dbtype'] == 'gridfs':
            handler = GridfsDict(name, config, repository)
        else:
            raise ValueError('unknown dbtype %s' % config['dbtype'])
        handler.load()
        return handler

    def load(self):
        """fetch dict from backend"""
        # if data is cached in local disk, load it
        if self.loadCache():
            return
        # fetch from backend server
        with self._lock:
            self.fetch()
        # cache the fetched data
        self.cache()

    def fetch(self):
        """fetch data from backend server"""
        raise NotImplementedError()

    def make(self, obj):
        """
            @Brief make get key/value pair from dict data obj
            @Param obj: dict data
                list for kv dict, column 0 is key field, value field is column 1 or None if only one column
                dict obj for json dict
        """
        if self.datatype == 'json':
            if not isinstance(obj, dict):
                raise ValueError('bad dict format')
            key_fields = self.config.get('key_field')
            if not key_fields:
                raise ValueError('key_field needs to be set')
            keys = []
            for key in key_fields:
                if key not in obj:
                    raise ValueError('%s missed in dict data' % key)
                keys.append(obj[key])
            if len(key_fields) == 1:
                key = keys[0]
            else:
                key = tuple(keys)
            value_field = self.config.get('value_field')
            if value_field:
                value = obj.get(value_field)
            else:
                value = obj
            return key, value
        else:
            if not isinstance(obj, list) or len(obj) < 1:
                raise ValueError('bad dict format')
            if len(obj) == 1:
                return obj[0], None
            else:
                return obj[0], obj[1]


    def loadCache(self):
        """loadCache"""
        if self.cache_path and os.path.exists(self.cache_path):
            with open(self.cache_path) as df:
                for line in df:
                    obj = json.loads(line)
                    key = obj['key']
                    if isinstance(key, list):
                        key = tuple(key)
                    self[key] = obj['value']
                LOG.info('dict[%s] loaded from cachefile[%s]' % (self.name, self.cache_path))
            return True
        return False

    def cache(self):
        if self.cache_path:
            with open(self.cache_path, 'w') as df:
                for k, v in self.iteritems():
                    print >>df, json.dumps({'key': k, 'value': v}, ensure_ascii = False)


class ElasticDict(DictObj):
    """ElasticDict
        Dict stored in elasticsearch
    """
    BATCH_SIZE = 1000
    def __init__(self, name, config, repository):
        """
            @Brief __init__
            @Param config:
            @Param repository:
        """
        super(ElasticDict, self).__init__(name, config, repository)
        self.index = config['index']
        self.doctype = config['doctype']

    def fetch(self):
        fetched = 0
        LOG.info('Loading dict[%s] from elasticsearch, [%s][%s]' % (self.name, self.index, self.doctype))
        with self.repository[self.backend].instance() as es:
            res = es.search(
                index = self.index,
                doc_type = self.doctype,
                scroll = '1m',
                size = self.BATCH_SIZE
                )
            scroll_id = res['_scroll_id']
            count = 0
            while True:
                for item in res['hits']['hits']:
                    key, value = self.make(item['_source'])
                    count += 1
                    self[key] = value

                if len(res['hits']['hits']) < self.BATCH_SIZE:
                    break
                res = es.scroll(scroll_id = scroll_id, scroll = '1m')
            es.clear_scroll(scroll_id = scroll_id)
            LOG.info('Loaded %d records for dict[%s]' % (count, self.name))

class MongoDict(DictObj):
    """MongoDict
        Dict stored in mongodb collection
    """
    def __init__(self, name, config, repository):
        super(MongoDict, self).__init__(name, config, repository)
        self.database = config['database']
        self.collection = config['collection']

    def fetch(self):
        LOG.info('Loading dict[%s] from [%s]' % (self.name, self.backend))
        with self.repository[self.backend].instance() as client:
            collection = client[self.database][self.collection]
            count = 0
            for doc in collection.find():
                count += 1
                key, value = self.make(doc)
                self[key] = value
            LOG.info('Loaded %d records for dict[%s]' % (count, self.name))

class GridfsDict(DictObj):
    """GridfsDict
        Dict stored in gridfs file
    """
    def __init__(self, name, config, repository):
        super(GridfsDict, self).__init__(name, config, repository)
        self.database = config['database']
        self.collection = config.get('collection', 'fs')
        self.filename = config['filename']

    def fetch(self):
        with self.repository[self.backend].instance() as client:
            fs = gridfs.GridFS(client[self.database], self.collection)
            df = fs.get_last_version(self.filename)
            LOG.info('Loading [%s] from gridfs, uploadTime[%s], md5[%s]' % (self.filename, df.upload_date, df.md5))
            count = 0
            while True:
                line = df.readline()
                if not line:
                    break
                count += 1
                if self.datatype == 'kv':
                    words = line.strip().split('\t')
                    key, value = self.make(words)
                    self[key] = value
                else:
                    obj = json.loads(line)
                    key, value = self.make(words)
                    self[key] = value

            LOG.info('Loaded %d records for dict[%s]' % (count, self.name))
