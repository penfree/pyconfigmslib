# encoding=utf8

""" The config section
    Author: lipixun
    Created Time : 日 12/ 6 22:39:31 2015

    File Name: section.py
    Description:

"""

from threading import RLock

class ConfigSection(object):
    """The config section
    """
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

class ReferConfigSection(ConfigSection):
    """The config section which support reference counter
    """
    def __init__(self, config):
        """Create a new ReferConfigSection
        """
        # Super
        super(ReferConfigSection, self).__init__(config)
        # Set
        self._lock = RLock()
        self._refcount = 0

    def __getrefvalue__(self):
        """Get the current referenced value
        """
        return self

    def __releaseref__(self):
        """The reference count has decreased to zero, could be released
        """
        pass

    def __enter__(self):
        """Enter with statement
        """
        with self._lock:
            self._refcount += 1
            return self.__getrefvalue__()

    def __exit__(self, type, value, tb):
        """Exit with statement
        """
        with self._lock:
            if self._refcount > 0:
                self._refcount -= 1
            # Release if no reference
            if self._refcount == 0:
                self.__releaseref__()

