# encoding=utf8

""" The config repository
    Author: lipixun
    Created Time : 日 12/ 6 22:40:44 2015

    File Name: repository.py
    Description:

"""

import logging

from os.path import dirname, abspath, join
from threading import Lock, Thread

import yaml
import requests

from sections import SECTIONS
from section import ConfigSection

KNOWN_SECTION_TYPES = {
        None:               ConfigSection,
        }
KNOWN_SECTION_TYPES.update(map(lambda x: (x.TYPE, x), SECTIONS))

class ConfigRepository(object):
    """The config repository
    """
    logger = logging.getLogger('configmslib.repository')

    def __init__(self, sectionTypes = None):
        """Create a new ConfigRepository
        """
        self.lock = Lock()
        self.sectionTypes = sectionTypes or KNOWN_SECTION_TYPES
        self.sections = {} # The loaded config section snapshot, key is config section name (A string), value is ConfigSectionSnapshot
        self.watchingNames = [] # The watching names, a list of WatchingName object
        self.hooks = {} # The hooks, key is WatchingName object, value is a list of callback method
        # The states
        self._watching = False
        self._thread = None
        self._request = None

    def __getitem__(self, section):
        """Get config section
        """
        sectionSnapshot = self.sections.get(section)
        if sectionSnapshot:
            return sectionSnapshot.value

    def __getattr__(self, section):
        """Get config section
        """
        return self[section]

    def __iter__(self):
        """Iterate this repository, return the config section snapshots
        """
        # NOTE: Here we're using the values not the itervalues in order to
        #       avoid exceptions when changing the sections in the iterating
        for sectionSnapshot in self.sections.values():
            yield sectionSnapshot

    @property
    def watching(self):
        """Return whether watching or not
        """
        return self._watching

    @property
    def default(self):
        """Return the default config section
        """
        return self[None]

    def watchAsync(self):
        """Start watch the config
        """
        # TODO: Start watch

    def hook(self, name, callback):
        """Add a hook
        """

    def gets(self, name):
        """Get the configs by WatchingName
        Parameters:
            name                        The name string or WatchingName
        Returns:
            Yield of config section
        """
        if isinstance(name, basestring):
            yield self[name]
        elif isinstance(name, NormalName):
            yield self[name.name]
        else:
            # Match one by one
            for key in self.sections.keys():
                if name.match(key):
                    snapshot = self.sections.get(key)
                    if snapshot:
                        yield snapshot.value

    def updateSection(self, name, value, timestamp):
        """Update the section
        Parameters:
            name                        The section name
            value                       The config value
            timestamp                   The new config value timestamp
        Returns:
            True if updated, otherwise false
        """
        with self.lock:
            _type, config = value.get('type'), value.get('config')
            # Check if we have created the section
            if name in self.sections and timestamp >= self.sections[name].timestamp:
                # OK, already created and we need to update the config, let's check if the type match
                if _type == self.sections[name].value.TYPE:
                    # Good, let's update the config
                    self.sections[name].value.update(config)
                    return True
                else:
                    # The type not match, we have to close the old one and create a new one
                    oldSection = self.sections.pop(name)
                    try:
                        oldSection.close()
                    except:
                        self.logger.exception('Failed to close config section [%s], skip', name)
                    # Create the new one
                    self.sections[name] = ConfigSectionSnapshot(name, self.createConfigSection(_type, config), timestamp)
                    # Done
                    return True
            else:
                # Create a new one
                self.sections[name] = ConfigSectionSnapshot(name, self.createConfigSection(_type, config), timestamp)
                return True
        # Done
        return False

    def createConfigSection(self, _type, config):
        """Create a new ConfigSection
        """
        if not _type in self.sectionTypes:
            raise ValueError('Section type [%s] not found' % _type)
        return self.sectionTypes[_type](config)

    def updateWatching(self, names):
        """Update the watching names
        """
        # TODO: Update watching

    def loadSchema(self, filename):
        """Load a schema from filename
        Parameters:
            filename                    The schema filename, should be in yaml format
        Returns:
            Nothing
        NOTE:
            - The schema specifys config sections and how to load them
            - The latest schema will overrwrite the section which is loaded previously
        """
        schemaFileDir = dirname(abspath(filename))
        # Load file
        with open(filename, 'rb') as fd:
            schema = yaml.load(fd)
        # Read schema
        watchingNames = []
        # Read static sections from schema
        sections = schema.get('sections')
        if sections:
            for section in sections:
                name, value, watching = section.get('name'), section.get('value'), section.get('watching', True)
                # Check parameters
                if not value:
                    raise ValueError('Require value of the section')
                # Update the section
                self.updateSection(name, value, 0)
                # Add watching name
                if watching:
                    name = NormalName(name)
                    if not name in watchingNames:
                        watchingNames.append(name)
        # Read watching from schema
        watching = schema.get('watching')
        if watching:
            for watchingName in watching:
                if not watchingName in watchingNames:
                    watchingNames.append(watchingName)
        self.updateWatching(watchingNames)
        # Done

    def doWatchingLoop(self):
        """Do the watching loop
        """
        pass

    def updateCallback(self, sectionSnapshot):
        """The config update callback
        This callback does:
            1. Update the config value
            2. Call the callback in hooks
        """
        pass

class WatchingName(object):
    """The watching name
    """
    def match(self, name):
        """Tell the name match the current watching name
        Parameters:
            name                    The name string
        Returns:
            True / False
        """
        raise NotImplementedError

    def dump(self):
        """Dump to json
        """
        raise NotImplementedError

class NormalName(WatchingName):
    """The normal name of loading section name
    """
    TYPE = 'normal'

    def __init__(self, name):
        """Create a new NormalName
        """
        self.name = name

    def __hash__(self):
        """Get the hash code
        """
        return hash(self.name)

    def __eq__(self, other):
        """Equals
        """
        return isinstance(other, NormalName) and self.name == other.name

    def __ne__(self, other):
        """Not equals
        """
        return not isinstance(other, NormalName) or self.name != other.name

    def match(self, name):
        """Tell the name match the current watching name
        Parameters:
            name                    The name string
        Returns:
            True / False
        """
        return self.name == name

    def dump(self):
        """Dump to json
        """
        return { 'type': self.TYPE, 'name': self.name }

    @classmethod
    def load(cls, js):
        """Load from json dict
        """
        return cls(js['name'])

class PrefixName(WatchingName):
    """The prefix name of loading section name
    NOTE:
        The name segment is separated by dot
    """
    TYPE = 'prefix'

    def __init__(self, prefix):
        """Create a new prefix
        """
        self.prefix = prefix

    def __hash__(self):
        """Get the hash code
        """
        return hash(self.prefix)

    def __eq__(self, other):
        """Equals
        """
        return isinstance(other, PrefixName) and self.prefix == other.prefix

    def __ne__(self, other):
        """Not equals
        """
        return not isinstance(other, PrefixName) or self.prefix != other.prefix

    def match(self, name):
        """Tell the name match the current watching name
        Parameters:
            name                    The name string
        Returns:
            True / False
        """
        return name.startswith(self.prefix)

    def dump(self):
        """Dump to json
        """
        return { 'type': self.TYPE, 'prefix': self.prefix }

    @classmethod
    def load(cls, js):
        """Load from json dict
        """
        return cls(js['prefix'])

NAME_TYPES = dict(map(lambda x: (x.TYPE, x), [ NormalName, PrefixName ]))

class ConfigSectionSnapshot(object):
    """The config section snapshot
    """
    def __init__(self, name, value, timestamp = None):
        """Create a new ConfigSectionSnapshot
        """
        self.name = name
        self.value = value
        self.timestamp = timestamp or 0

