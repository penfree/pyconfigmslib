#!/usr/bin/env python
# coding=utf8
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
import hashlib


LOG = logging.getLogger("configms.sections.dict")
DEFAULT_CACHE_DIR = '/tmp/.bdmd/.dict'
NoDefault = object()


class DictConfigSection(ReferConfigSection):

    """DictConfigSection
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
            - enable_cache: whether the dict will be cached to disk to avoid downloading everytime
            - cache_path: cache directory, use different dir for modules
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
            if not value.get('filename'):
                raise ValueError('filename is required')
        else:
            raise ValueError('unknown dbtype')

    def reference(self, config):
        """
            @Brief reference
            @Param config:
        """
        return DictObj.getDict(self.key, config, self.repository)

    def reload(self, config):
        """
            @Brief reload Disable reload, dict can only load once
            @Param config:
        """
        if hasattr(self, '_value') and self._value:
            return
        else:
            super(DictConfigSection, self).reload(config)

    def release(self, value):
        """
            @Brief release
            @Param value:
        """
        self._value.value.clear()


def ensuredirs(path, mode=None):
    """
        @Brief ensuredirs an alternative for os.makedirs, can change mode for all
            directories created by this function
        @Param path:
        @Param mode:
    """
    if os.path.exists(path):
        return
    parent = os.path.dirname(path)
    ensuredirs(parent, mode)
    os.mkdir(path)
    if mode is not None:
        os.chmod(path, mode)


class DictObj(dict):
    """DictObj"""

    def __init__(self, name, config, repository):
        self.md5 = None
        self.name = name
        self.config = config
        self.repository = repository
        self.dbtype = config.get('dbtype')
        self.backend = config['backend']
        if self.dbtype == 'mongodb' or self.dbtype == 'elasticsearch':
            self.datatype = 'json'
        else:
            self.datatype = config.get('datatype', 'json')
        self.enable_cache = config.get('enable_cache', False)
        cache_path = config.get('cache_path', DEFAULT_CACHE_DIR)
        if cache_path == DEFAULT_CACHE_DIR:
            ensuredirs(cache_path, 0o777)
        else:
            ensuredirs(cache_path)
        if cache_path:
            self.cache_path = os.path.join(cache_path, self.name)
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
        """load dicts"""
        # Clear old data
        self.clear()
        # if data is cached in local disk, load it
        if self.enable_cache and self.loadCache():
            return
        # fetch from backend server
        with self._lock:
            self.fetch()
        # cache the fetched data
        if self.enable_cache:
            self.cache()

    def fetch(self):
        """fetch data from backend server"""
        raise NotImplementedError()

    def find(self, obj, key):
        """
            @Brief find key in obj
            @Param obj:
            @Param key:
        """
        def iterfind(obj, names):
            """Iterate find names in obj
            """
            if len(names) > 0:
                name = names[0]
                # Check the obj
                if isinstance(obj, dict):
                    # Find key in this dict
                    if name in obj:
                        # Good
                        for v in iterfind(obj[name], names[1:]):
                            yield v
                elif isinstance(obj, (list, tuple)):
                    # A list or tuple, iterate item
                    for item in obj:
                        for v in iterfind(item, names):
                            yield v
                else:
                    # Not a dict, list or tuple, stop here
                    pass
            else:
                yield obj
        for value in iterfind(obj, key.split(".")):
            yield value

    def getValue(self, obj, key, default=NoDefault):
        """
            @Brief getValue get field from obj by key
            @Param obj:
            @Param key:
        """
        values = list(self.find(obj, key))
        if not values:
            if default == NoDefault:
                raise ValueError(
                    'Cannot find Key[%s] in data[%s]' %
                    (key, json.dumps(
                        obj, ensure_ascii=False)))
            else:
                return default
        elif len(values) > 1:
            raise ValueError('There is more then one value for key[%s]' % key)
        else:
            return values[0]

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
                val = self.getValue(obj, key)
                keys.append(val)
            if len(key_fields) == 1:
                key = keys[0]
            else:
                key = tuple(keys)
            value_field = self.config.get('value_field')
            if value_field:
                value = self.getValue(obj, value_field)
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
                LOG.info(
                    'dict[%s] loaded from cachefile[%s]' %
                    (self.name, self.cache_path))
            return True
        return False

    def cache(self):
        if self.cache_path:
            with open(self.cache_path, 'w') as df:
                for k, v in self.iteritems():
                    print >>df, json.dumps(
                        {'key': k, 'value': v}, ensure_ascii=False)
            os.chmod(self.cache_path, 0o777)


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
        LOG.info(
            'Loading dict[%s] from elasticsearch, [%s][%s]' %
            (self.name, self.index, self.doctype))
        with self.repository[self.backend].instance() as es:
            res = es.search(
                index=self.index,
                doc_type=self.doctype,
                scroll='1m',
                size=self.BATCH_SIZE
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
                res = es.scroll(scroll_id=scroll_id, scroll='1m')
            es.clear_scroll(scroll_id=scroll_id)
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
        self.database = config.get('database', 'fs')
        self.collection = config.get('collection', 'fs')
        self.filename = config['filename']

    def parseLine(self, line):
        """
            @Brief parseLine 解析文件的一行
            @Param line:
        """
        if self.datatype == 'kv':
            words = line.strip().split('\t')
            key, value = self.make(words)
            self[key] = value
        else:
            obj = json.loads(line)
            key, value = self.make(obj)
            self[key] = value

    def fetch(self):
        with self.repository[self.backend].instance() as client:
            fs = gridfs.GridFS(client[self.database], self.collection)
            df = fs.get_last_version(self.filename)
            LOG.info(
                'Loading [%s] from gridfs, uploadTime[%s], md5[%s]' %
                (self.filename, df.upload_date, df.md5))
            self.md5 = df.md5
            count = 0
            cache_file = None
            if self.cache_path and self.enable_cache:
                cache_file = open(self.cache_path, 'w')
            while True:
                line = df.readline()
                if not line:
                    break
                count += 1
                self.parseLine(line)
                if cache_file is not None:
                    cache_file.write(line)
            if cache_file is not None:
                cache_file.close()
                os.chmod(self.cache_path, 0o777)
            df.close()

            LOG.info('Loaded %d records for dict[%s]' % (count, self.name))

    def loadCache(self):
        """loadCache"""
        if self.cache_path and os.path.exists(self.cache_path):
            local_md5 = self.md5sum(self.cache_path)
            with self.repository[self.backend].instance() as client:
                fs = gridfs.GridFS(client[self.database], self.collection)
                df = fs.get_last_version(self.filename)
                if df.md5 == local_md5:
                    LOG.info(
                        'file[%s] has not changed, will use local cache dict' %
                        self.filename)
                    self.md5 = local_md5
                else:
                    LOG.info(
                        'file[%s] has changed, local_md5:%s != remote_md5:%s' %
                        (self.filename, local_md5, df.md5))
                    return False

            with open(self.cache_path) as df:
                for line in df:
                    self.parseLine(line)
                LOG.info(
                    'dict[%s] loaded from cachefile[%s]' %
                    (self.name, self.cache_path))
            return True
        return False

    def cache(self):
        pass

    def md5sum(self, fname):
        """
            @Brief md5sum 计算文件md5
            @Param fname: 文件路径或文件流
        """
        def read_chunks(fh):
            fh.seek(0)
            chunk = fh.read(8096)
            while chunk:
                yield chunk
                chunk = fh.read(8096)
            else:
                fh.seek(0)
        m = hashlib.md5()
        if isinstance(fname, basestring) and os.path.exists(fname):
            with open(fname, "rb") as fh:
                for chunk in read_chunks(fh):
                    m.update(chunk)
        elif fname.__class__.__name__ in ["StringIO", "StringO"] or isinstance(fname, file):
            for chunk in read_chunks(fname):
                m.update(chunk)
        else:
            return ""
        return m.hexdigest()
