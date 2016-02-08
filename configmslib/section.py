# encoding=utf8

""" The config section
    Author: lipixun
    Created Time : 日 12/ 6 22:39:31 2015

    File Name: section.py
    Description:

"""

import logging

from threading import RLock
from contextlib import contextmanager

class ConfigSection(object):
    """The config section
    """
    TYPE = None

    def __init__(self, config):
        """Create a new ConfigSection
        Parameters:
            config                              The config dict
        """
        self.config = config

    def __len__(self):
        """Get the length
        """
        return len(self.config)

    def __contains__(self, key):
        """Check if a key exists
        """
        return key in self.config

    def __getitem__(self, key):
        """Get a config value
        """
        return self.config[key]

    def __setitem__(self, key, value):
        """Set a config value
        """
        self.config[key] = value

    def __delitem__(self, key):
        """Delete a config value
        """
        del self.config[key]

    def get(self, key, default = None):
        """Get a config
        """
        return self.config.get(key, default)

    def update(self, config):
        """Update the config
        Parameters:
            config                              The config dict
        Returns:
            Nothing
        """
        self.config = config

    def close(self):
        """Close the config
        """

class ReferConfigSection(ConfigSection):
    """The config section which support reference counter
    """
    logger = logging.getLogger('config.refer')

    def __init__(self, config):
        """Create a new ReferConfigSection
        """
        self._lock = RLock()
        # Get reference value
        self._value = ReferencedValue(self.__reference__(config))
        # Super
        super(ReferConfigSection, self).__init__(config)

    def __reference__(self, config):
        """Get the current referenced value
        """
        raise NotImplementedError

    def __release__(self, value):
        """The reference count has decreased to zero, could be released
        """
        pass

    def __withinerror__(self, error):
        """When error occurred in the with statements
        """
        pass

    def __getinstancevalue__(self, value):
        """Get the value returned by instance method
        """
        yield value.value

    @contextmanager
    def instance(self):
        """Get the instance
        """
        value = self._value
        # Increase
        value.increase()
        try:
            # Yield
            for instanceValue in self.__getinstancevalue__(value):
                yield instanceValue
        except Exception as error:
            # Run within error
            self.__withinerror__(error)
            # Re-raise
            raise
        finally:
            # Decrease
            value.decrease()
            # Check
            if self._value != value and value.notReferenced:
                # Updated and should be release
                try:
                    self.__release__(value.value)
                except:
                    self.logger.exception('Failed to release the referenced value')

    def update(self, config):
        """Update the config
        Parameters:
            config                              The config dict
        Returns:
            Nothing
        """
        # Create value
        self._value = ReferencedValue(self.__reference__(config))
        # Super
        return super(ReferConfigSection, self).update(config)

class ReferencedValue(object):
    """The referenced value
    """
    def __init__(self, value):
        """Create a new ReferencedValue object
        """
        self._lock = RLock()
        self._refcount = 0
        self._value = value

    @property
    def notReferenced(self):
        """Tell if current value is not referenced
        """
        return self._refcount == 0

    @property
    def value(self):
        """The referenced value
        """
        return self._value

    def increase(self):
        """Increase the reference counter
        """
        with self._lock:
            self._refcount += 1

    def decrease(self):
        """Decrease the reference counter
        """
        with self._lock:
            self._refcount -= 1
