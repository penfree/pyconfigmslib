# encoding=utf8

""" The library config
    Author: lipixun
    Created Time : 日  8/ 7 20:48:50 2016

    File Name: config.py
    Description:

"""

import etcd

from spec import ConfigEtcd, ConfigEnviron

class EtcdConfig(dict):
    """The global config
    """
    def __init__(self, **kwargs):
        """Create a new EtcdConfig
        """
        super(EtcdConfig, self).__init__(kwargs)
        # Create etcd client
        srvDomain = kwargs.pop("srvDomain", None)
        if srvDomain:
            # Use service domain
            self._client = etcd.Client(srv_domain = srvDomain, allow_reconnect = True, **self.popOptionalParams(kwargs))
        else:
            # Use host / port
            hosts = kwargs.pop("hosts", None)
            if not hosts:
                raise ValueError("Etcd config requires either srvDomain or host")
            _hosts = []
            for host in hosts:
                if isinstance(host, basestring):
                    _hosts.append((host, 2379))
                else:
                    _host.append((host["host"], host["port"]))
            self._client = etcd.Client(host = tuple(_hosts), allow_reconnect = True, **self.popOptionalParams(kwargs))

    @property
    def client(self):
        """Get the etcd client
        """
        return self._client

    @classmethod
    def popOptionalParams(cls, kwargs):
        """
        """
        params = {}
        prefix = kwargs.pop("prefix", None)
        if prefix:
            params["version_prefix"] = prefix
        redirect = kwargs.pop("redirect", None)
        if not redirect is None:
            params["allow_redirect"] = redirect
        scheme = kwargs.pop("scheme", "http")
        if scheme:
            params["protocol"] = scheme
        # Check kwargs
        if kwargs:
            raise ValueError("Has unknown parameters: [%s]", ",".join(kwargs.iterkeys()))
        # Good
        return params

class EnvironConfig(dict):
    """The environ config
    """
    DefaultRepo = "system/config"
    DefaultName = "default"

    @property
    def repository(self):
        """Get the repository name
        """
        return self.get("repository", self.DefaultRepo)

    @property
    def name(self):
        """Get the environ name
        """
        return self.get("name", self.DefaultName)

    def getEtcdPath(self, key):
        """Get the etcd path
        """
        repo = self.repository
        if not repo.endswith("/"):
            path = "%s/%s" % (repo, self.name)
        else:
            path = repo + self.name
        if not path.endswith("/"):
            return "%s/%s" % (path, key)
        else:
            return path + key

GlobalConfigs = {
    ConfigEtcd: EtcdConfig,
    ConfigEnviron: EnvironConfig,
}
