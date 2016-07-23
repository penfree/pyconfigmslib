# encoding=utf8

""" The config repository
    Author: lipixun
    Created Time : 日 12/ 6 22:40:44 2015

    File Name: repository.py
    Description:

"""

import logging

from os.path import dirname, abspath, join

import yaml

from sections import SECTIONS
from section import ConfigSection

KNOWN_SECTION_TYPES = {
        None: ConfigSection,
        }
KNOWN_SECTION_TYPES.update(map(lambda x: (x.TYPE, x), SECTIONS))

class ConfigRepository(object):
    """The config repository
    """
    logger = logging.getLogger('configmslib.repository')

    def __init__(self, sectionTypes = None):
        """Create a new ConfigRepository
        """
        self.sectionTypes = sectionTypes or KNOWN_SECTION_TYPES
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

    def get(self, name):
        """Get a config value
        """
        return self[name]

    def updateSection(self, name, value):
        """Update the section
        Parameters:
            name                        The section name
            value                       The config value
        """
        _type, config = value.get('type'), value.get('config')
        if not _type in self.sectionTypes:
            raise ValueError('Section type [%s] not found' % _type)
        self.sections[name] = self.sectionTypes[_type](config)

    def loadSchema(self, filename, environPath = None):
        """Load a schema from filename
        Parameters:
            filename                    The schema filename, should be in yaml format
            environPath                 The environment path, used when resolving includes. Use the same directory as the filename when not specified
        Returns:
            Nothing
        NOTE:
            - The schema specifys config sections and how to load them
            - The latest schema will overrwrite the section which is loaded previously
        """
        filename = abspath(filename)
        environPath = dirname(filename)
        loadedFiles = { filename }
        # Load the file
        self._loadSchemaFile(filename, environPath, loadedFiles)

    def _loadSchemaFile(self, filename, environPath, loadedFiles):
        """Load the schema file
        Returns:
            Nothing
        """
        # Load file
        with open(filename, 'rb') as fd:
            schema = yaml.load(fd)
            # Read the includes
            includes = schema.get('includes')
            if includes:
                for includeFilename in includes:
                    if includeFilename.startswith('.'):
                        # Force relative to current file
                        includeFilename = join(dirname(filename), includeFilename)
                    elif includeFilename.startswith('/'):
                        # Force abspath
                        pass
                    else:
                        includeFilename = join(environPath, includeFilename)
                    # Load it
                    if not includeFilename in loadedFiles:
                        loadedFiles.add(includeFilename)
                        self._loadSchemaFile(includeFilename, environPath, loadedFiles)
            # Read static sections from schema
            sections = schema.get('sections')
            if sections:
                for section in sections:
                    name, value = section.get('name'), section.get('value')
                    # Check parameters
                    if not value:
                        raise ValueError('Require value of the section')
                    # Update the section
                    self.updateSection(name, value)
