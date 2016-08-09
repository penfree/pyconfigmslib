# encoding=utf8

""" The config repository
    Author: lipixun
    Created Time : 日 12/ 6 22:40:44 2015

    File Name: repository.py
    Description:

"""

import logging

from sets import Set
from os.path import dirname, abspath, join, basename

import yaml

from spec import ConfigEtcd, ConfigEnviron
from config import GlobalConfigs, EnvironConfig
from section import ConfigSection
from sections import SECTIONS

KNOWN_SECTION_TYPES = {
        None: ConfigSection,
        }
KNOWN_SECTION_TYPES.update(map(lambda x: (x.Type, x), SECTIONS))

class ConfigRepository(object):
    """The config repository
    """
    logger = logging.getLogger('configmslib.repository')

    def __init__(self, sectionTypes = None, enableEtcd = True):
        """Create a new ConfigRepository
        """
        self.sectionTypes = sectionTypes or KNOWN_SECTION_TYPES
        self.enableEtcd = enableEtcd
        self.globals = {}           # The global configs
        self.sections = {}

    def __getitem__(self, section):
        """Get config section
        """
        return self.sections.get(section)

    def __getattr__(self, section):
        """Get config section
        """
        return self[section]

    def __iter__(self):
        """Iterate this repository, return the config section snapshots
        """
        for section in self.sections.values():
            yield section

    @property
    def default(self):
        """Return the default config section
        """
        return self[None]

    @property
    def etcd(self):
        """Get the etcd client
        """
        etcdConfig = self.globals.get(ConfigEtcd)
        if etcdConfig:
            return etcdConfig.client

    @property
    def environ(self):
        """Get environ
        """
        environConfig = self.globals.get(ConfigEnviron)
        if environConfig:
            return environConfig
        else:
            return EnvironConfig()

    def get(self, name):
        """Get a config value
        """
        return self[name]

    def loadSchema(self, filename, environPath = None, noSections = False):
        """Load a schema from filename
        Parameters:
            filename                    The schema filename, should be in yaml format
            environPath                 The environment path, used when resolving includes. Use the same directory as the filename when not specified
            noSections                  Whether to load sections or not
        Returns:
            Nothing
        NOTE:
            - The schema specifys config sections and how to load them
            - The latest schema will overrwrite the section which is loaded previously
        """
        if not environPath:
            environPath = dirname(abspath(filename))
            filename = basename(filename)
        # Load the file
        self._loadSchemaFile(filename, environPath, noSections, Set())

    def _loadSchemaFile(self, filename, environPath, noSections, loadedFiles):
        """Load the schema file
        Returns:
            Nothing
        """
        # Add current file
        loadedFiles.add(filename)
        # Load file
        with open(filename if filename.startswith("/") else join(environPath, filename), "rb") as fd:
            schema = yaml.load(fd)
            # Read the includes
            includes = schema.get("includes")
            if includes:
                for includeFilename in includes:
                    if includeFilename.startswith("."):
                        # Relative to current file
                        includeFilename = join(dirname(filename), includeFilename)
                    # Load it if not loaded
                    if not includeFilename in loadedFiles:
                        self._loadSchemaFile(includeFilename, environPath, loadedFiles)
            # Read global configs
            globalConfigs = schema.get("globals")
            if globalConfigs:
                # Load global configs
                for key, value in globalConfigs.iteritems():
                    if key == ConfigEtcd and not self.enableEtcd:
                        self.logger.info("Found etcd config, ignored by etcd disabled")
                        continue
                    t = GlobalConfigs.get(key)
                    if not t:
                        raise ValueError("Unknown global config [%s] in file [%s]" % (key, filename))
                    if not isinstance(value, dict):
                        raise ValueError("Invalid global config value type [%s] of key [%s] in file [%s], expect [dict]" % (
                            type(value).__name,
                            key,
                            filename
                            ))
                    self.globals[key] = t(**value)
            # Read static sections from schema
            if not noSections:
                sections = schema.get("sections")
                if sections:
                    for sectionConfig in sections:
                        # Load the section
                        section = self.loadSection(sectionConfig)
                        if section.key in self.sections:
                            raise ValueError("Conflict section [%s]", section.key)
                        self.sections[section.key] = section

    def loadSection(self, section):
        """Update the section
        Parameters:
            name                        The section name
            value                       The config value
        """
        name, t, value, autoUpdate, environ, wait = \
            section.get("name"), \
            section.get("type"), \
            section.get("value"), \
            section.get("autoUpdate", False), \
            section.get("environ"), \
            section.get("wait", True)
        # Check
        if not name:
            name = None
        if not t in self.sectionTypes:
            raise ValueError("Unknown section type [%s]" % t)
        if environ:
            environ = EnvironConfig(**environ)
        # Create
        return self.sectionTypes[t](name, value, self, autoUpdate, environ, wait)
