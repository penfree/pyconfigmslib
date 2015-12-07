# encoding=utf8

""" The config repository
    Author: lipixun
    Created Time : 日 12/ 6 22:40:44 2015

    File Name: repository.py
    Description:

"""

import logging

from threading import Thread, Lock, Event
from os.path import dirname, abspath, join
from collections import namedtuple

import yaml

from unifiedrpc import Service, endpoint

from sections import SECTIONS
from section import ConfigSection

KNOWN_SECTION_TYPES = {
        None:               ConfigSection,
        }
KNOWN_SECTION_TYPES.update(map(lambda x: (x.TYPE, x), SECTIONS))

class ConfigRepository(Service):
    """The config repository
    NOTE:
        This is also an unified rpc service
    """
    logger = logging.getLogger('configmslib.repository')

    def __init__(self, sectionTypes = None):
        """Create a new ConfigRepository
        """
        self.sectionTypes = sectionTypes or KNOWN_SECTION_TYPES
        self.lock = Lock()
        self.initEvent = None
        self.initCount = 0
        self.initThreads = []
        self.watchingRepos = {} # Key is watching repo name, value is ConfigSection
        self.sections = {}  # Key is section name, value is LoadedConfigSection

    def __getitem__(self, section):
        """Get config section
        """
        loadedConfigSection = self.sections.get(section)
        if loadedConfigSection:
            return loadedConfigSection.value

    def __getattr__(self, section):
        """Get config section
        """
        return self[section]

    @property
    def default(self):
        """Return the default config section
        """
        return self[None]

    def bootup(self, adapter):
        """Bootup this service
        """
        if adapter.type != 'rabbitmq':
            return
        # Create a channel
        # TODO:
        raise NotImplementedError

    @endpoint()
    def onWatchingRepositoryChanged(self, routingKey, data, message, publish, ack):
        """On watching repository changed
        """
        raise NotImplementedError

    def loadSectionByConfig(self, config):
        """Load the section by config
        """
        type, config = config.get('type'), config['config']
        if not type in self.sectionTypes:
            raise ValueError('Unknown config section type [%s]' % type)
        # Create the config section
        return self.sectionTypes[type](config)

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
        sections = schema.get('sections')
        if not sections:
            return
        # Load each section
        for section in sections:
            # Get section parameters
            sectionName, repoName, filename, value, watching = \
                    section.get('name'), section.get('repoName'), section.get('filename'), section.get('value'), section.get('watching')
            if sectionName in self.sections:
                # Unload the section
                self.unload(sectionName)
            # Get the section type, by repo, by file or by value
            if repoName and not filename and not value:
                # By repo
                self.load(repoName, sectionName, watching if not watching is None else True)
            elif filename and not repoName and not value:
                # By file
                # NOTE: if the filename is not an absolute path, will use the relative path of the schema file
                if not filename.startswith('/'):
                    # Make relative to absolute path
                    filename = join(schemaFileDir, filename)
                self.loadFile(filename, sectionName)
            elif not value is None and not repoName and not filename:
                # By value
                self.loadDict(value, sectionName)
            else:
                raise ValueError('Invalid section [%s] please provide one and only one of the parameters [repoName, filename, value]' % sectionName)

    def loadDict(self, configDict, section = None):
        """Load a config dict
        Parameters:
            configDict                  The config dict
            section                     The section to be loaded into, None means the default section
        Returns:
            The loaded ConfigSection object
        """
        with self.lock:
            if section in self.sections:
                raise ValueError('Section [%s] already exists' % section)
            # Create & add section
            configSection = self.loadSectionByConfig(configDict)
            self.sections[section] = LoadedConfigSection(configSection, None, None)
            # Done
            return configSection

    def loadFile(self, filename, section = None):
        """Load a config section file
        Parameters:
            filename                    The config filename, should be in yaml format
            section                     The section to be loaded into, None means the default section
        Returns:
            The loaded ConfigSection object
        """
        with self.lock:
            if section in self.sections:
                raise ValueError('Section [%s] already exists' % section)
            # Load file
            with open(filename, 'rb') as fd:
                config = yaml.load(fd)
            # Create & add the section
            configSection = self.loadSectionByConfig(config)
            self.sections[section] = LoadedConfigSection(configSection, None, None)
            # Done
            return configSection

    def load(self, repoName, section = None, watching = True):
        """Load from config management service
        Parameters:
            repoName                    The repo name in config management service
            section                     The section to be loaded into, None means the default section
            watching                    Keep watching the configuration modification
        Returns:
            The loaded ConfigSection object
        """
        with self.lock:
            if watching and repoName in self.watchingRepos:
                raise ValueError('Repository [%s] is already watching' % repoName)
            if section in self.sections:
                raise ValueError('Section [%s] already exists' % section)
            # Add it
            loadedConfigSection = LoadedConfigSection(None, repoName, watching)
            self.sections[section] = loadedConfigSection
            if watching:
                self.watchingRepos[repoName] = loadedConfigSection
            # Initialize the repo
            thread = Thread(self.initRepo4ConfigSection, loadedConfigSection)
            thread.start()
            self.initThreads.append(thread)
            # Add init count
            self.initCount += 1
            # Check event
            if not self.initEvent:
                self.initEvent = Event()

    def unload(self, section = None):
        """Unload the section
        Parameters:
            section                     The section name, None means the default section
        Returns:
            Nothing
        """
        loadedConfigSection = self.sections.get(section)
        if loadedConfigSection:
            # Delete the section from sections
            del self.sections[section]
            # Unwatch if necessary
            if loadedConfigSection.repoName and loadedConfigSection.watching and loadedConfigSection.repoName in self.watchingRepos:
                # Delete the watching entry from watch repos
                del self.watchingRepos[loadedConfigSection.repoName]
                # Unbind

    def initRepo4ConfigSection(self, loadedConfigSection):
        """Initialize repo for config section
        """
        try:
            # TODO: Add rpc call to config api server
            pass
        except:
            pass
        finally:
            # Decrease init count
            with self.lock:
                self.initCount -= 1
                if self.initCount == 0:
                    # Set event
                    self.initEvent.set()
                    self.initEvent = None

    def wait4init(self, timeout = None):
        """Wait for initialize completed
        Parameters:
            timeout                     The timeout in seconds
        """
        event = self.initEvent
        if not event:
            # No initialization is in progress
            return
        if not event.wait(timeout):
            # Timeout
            raise TimeoutError
        # Good, join threads
        # NOTE: Here, we have potential multi-thread problem
        with self.lock:
            for thread in self.initThreads:
                thread.join()
            self.initThreads = []

class LoadedConfigSection(object):
    """The loaded config section
    """
    def __init__(self, value, repoName, watching):
        """Create a new LoadedConfigSection
        """
        self.value = value
        self.repoName = repoName
        self.watching = watching


