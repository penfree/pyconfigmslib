# encoding=utf8

""" The config utilty
    Author: lipixun
    Created Time : 一  8/ 8 08:19:59 2016

    File Name: __main__.py
    Description:

"""

import sys
reload(sys)
sys.setdefaultencoding("utf8")

import logging
logging.basicConfig(format = '%(asctime)s - %(levelname)s - %(name)s - %(message)s', level = logging.INFO)

import json

import etcd
import yaml
import termcolor

from spec import ConfigEtcd, ConfigEnviron
from config import EnvironConfig
from repository import ConfigRepository

from argparse import ArgumentParser

def getArguments():
    """Get arguments
    """
    parser = ArgumentParser(description = "Config set utility")
    parser.add_argument("--scheme", dest = "scheme", default = "http", help = "Etcd scheme")
    parser.add_argument("--host", dest = "host", help = "Etcd host")
    parser.add_argument("--port", dest = "port", type = int, default = 2379, help = "Etcd port")
    parser.add_argument("--srvdomain", dest = "srvDomain", help = "Etcd service domain")
    parser.add_argument("--repo", dest = "repository", help = "The repository")
    parser.add_argument("--env", dest = "environ", help = "The environment")
    parser.add_argument("--config", dest = "config", help = "The config file")
    subParsers = parser.add_subparsers(dest = "action")
    setSubParser = subParsers.add_parser("set", help = "Set config")
    setSubParser.add_argument("kvs", nargs = "+", help = "Config key values. Format: key=value [key=value]*")
    getSubParser = subParsers.add_parser("get", help = "Get config")
    getSubParser.add_argument("keys", nargs = "+", help = "Config keys, Format: key [key]*")
    return parser.parse_args()

def getEtcdClientByConfig(configFilename):
    """Get etcd client by config
    """
    repository = ConfigRepository()
    # Load config without sections
    repository.loadSchema(configFilename, noSections = True)
    # Return
    return repository.etcd, repository.environ

def getEtcdClientBySystemSearchDomain(scheme):
    """Get etcd client by system search domain
    """
    domains = []
    with open("/etc/resolv.conf", "rb") as fd:
        for content in fd:
            content = content.strip()
            if content.startswith("search "):
                domains.extend(filter(lambda x: x, map(lambda x: x.strip(), content.split(" ")[1: ])))
    for domain in domains:
        # Try
        logging.info("Try to connect to etcd by service domain [%s]", domain)
        try:
            return etcd.Client(srv_domain = domain, allow_reconnect = True, protocol = scheme)
        except:
            logging.info("Failed to connect to etcd")
    # Not found
    raise ValueError("No available system domain found")

def loadValueFile(filename):
    """Load value file
    """
    # Determind the filetype
    if filename.endswith(".json"):
        loader = json.load
    elif filename.endswith(".yaml") or filename.endswith(".yml"):
        loader = yaml.load
    else:
        logging.warn("Cannot determind the file type according to filename, try to load as json file")
        loader = json.load
    # Load
    with open(filename, "rb") as fd:
        value = loader(fd)
        # Good, json dump
        return json.dumps(value, ensure_ascii = False)

def main():
    """The main entry
    """
    args = getArguments()
    # Connect etcd, will try in the following order:
    #   - config
    #   - host
    #   - service domain
    #   - system search domain as service domain
    environ = None
    try:
        if args.config:
            logging.info("Try to connect to etcd by config [%s]", args.config)
            etcdClient, environ = getEtcdClientByConfig(args.config)
        elif args.host:
            logging.info("Try to connect to etcd by host [%s] port [%d]", args.host, args.port)
            etcdClient = etcd.Client(host = args.host, port = args.port, allow_reconnect = True, protocol = args.scheme)
        elif args.srvDomain:
            logging.info("Try to connect to etcd by service domain [%s]", args.srvDomain)
            etcdClient = etcd.Client(srv_domain = args.srvDomain, allow_reconnect = True, protocol = args.scheme)
        else:
            logging.info("Try to connect to etcd by system search domain")
            etcdClient = getEtcdClientBySystemSearchDomain(args.scheme)
    except Exception as error:
        logging.error("Failed to connect to etcd, error: [%s]", error)
        return 1
    # Get environ
    if not environ:
        environ = EnvironConfig()
    if args.repository:
        environ["repository"] = args.repository
    if args.environ:
        environ["name"] = args.environ
    logging.info("Set environ [%s] --> [%s]", environ.repository, environ.name)
    try:
        if args.action == "set":
            # Set the config
            for kv in args.kvs:
                idx = kv.find("=")
                if idx == -1:
                    logging.error("Bad kv value: [%s]", kv)
                    return 1
                key, value = kv[: idx], kv[idx + 1: ]
                if value.startswith("@"):
                    # From file
                    logging.info("Load value of key [%s] from file [%s]", key, value)
                    value = loadValueFile(value[1: ])
                print "Set key", termcolor.colored(key, "yellow")
                etcdClient.write(environ.getEtcdPath(key), value)
        elif args.action == "get":
            # Get the config
            for key in args.keys:
                result = etcdClient.read(environ.getEtcdPath(key))
                print termcolor.colored(key, "yellow"), result.value
        else:
            logging.error("Unknown action [%s]", args.action)
            return 1
    except etcd.EtcdKeyNotFound:
        # Config not found
        print termcolor.colored("Config not found", "red")
    # Done
    return 0

sys.exit(main())
