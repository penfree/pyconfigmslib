# encoding=utf8

""" The config section
    Author: lipixun
    Created Time : 日 12/ 6 22:39:31 2015

    File Name: section.py
    Description:

        Update section from etcd:

            The config is stored at path: <environ.repository>/<environ.name>/<key>

"""

import time
import logging

from etcd import EtcdKeyNotFound, EtcdWatchTimedOut, EtcdEventIndexCleared

from util import json
from config import EnvironConfig
from threading import Thread, RLock, Event
from contextlib import contextmanager

NoDefault = object()

class ConfigSection(dict):
    """The config section
    """
    logger = logging.getLogger("configmslib.section")

    Type = None
    ReloadRequired = False

    def __init__(self, key, value, repository, autoUpdate = False, environ = None, wait = False):
        """Create a new ConfigSection
        Parameters:
            key                             The config key
            value                           The config value
            autoUpdate                      Auto update this config or not
            environ                         The environ config dict
        """
        self._key = key
        self._repository = repository
        self._autoUpdate = autoUpdate
        self._environ = EnvironConfig(**environ) if environ else None
        self._updatedEvent = Event()
        self._timestamp = 0.0
        self._reloadLock = None
        self._reloadEvent = None
        self._reloadedEvent = None
        self._reloadThread = None
        self._autoUpdateThread = None
        # Super
        super(ConfigSection, self).__init__()
        # Check if reload is required
        if self.ReloadRequired:
            self._reloadLock = RLock()
            self._reloadEvent = Event()
            self._reloadedEvent = Event()
            # Start reload thread
            thread = Thread(target = self.__reload__)
            thread.setDaemon(True)
            thread.start()
            self._reloadThread = thread
        # Update the value
        try:
            value = self.getInitialUpdatedValue()
        except:
            if self.logger.isEnabledFor(logging.DEBUG):
                self.logger.exception("Failed to get initial updated value, fall back to pre-defined value")
        self.update(value)
        # Check auto update (Key will empty value is not be auto updated)
        if key and autoUpdate:
            thread = Thread(target = self.__autoupdate__)
            thread.setDaemon(True)
            thread.start()
            self._autoUpdateThread = thread
        # Wait
        if wait:
            if self.ReloadRequired and self._reloadedEvent:
                # Wait for reloaded
                self._reloadedEvent.wait()
            elif self._updatedEvent:
                # Wait for updated
                self._updatedEvent.wait()

    @property
    def key(self):
        """Get the section key
        """
        return self._key

    @property
    def repository(self):
        """Get the config repository this section belongs to
        """
        return self._repository

    def first(self, key, default = NoDefault):
        """Get first value of key
        """
        for value in self.find(key):
            return value
        # Not found
        if default == NoDefault:
            raise KeyError(key)
        return default

    def find(self, key):
        """Find the values of key
        Returns:
            Yield of value
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
                        for v in iterfind(obj[name], names[1: ]):
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
        for value in iterfind(self, key.split(".")):
            yield value

    def __autoupdate__(self):
        """Auto update
        """
        # TODO: Support modified index
        initialized = False
        self.logger.debug("[%s] Auto update thread started", self.Type)
        while True:
            # Get etcd client
            client = None
            while True:
                if self._repository.etcd is None:
                    self.logger.error("[%s] Failed to watch config, no etcd client found, will retry in 30s", self.Type)
                    time.sleep(30)
                    continue
                client = self._repository.etcd
                break
            # Wait for the config
            # Get the read path
            if self._environ:
                path = self._environ.getEtcdPath(self._key)
            else:
                path = self._repository.environ.getEtcdPath(self._key)
            # Wait the config
            try:
                if not initialized:
                    # Not initialized
                    self.logger.debug("[%s] Watching config at path [%s]", self.Type, path)
                    if self.update(json.loads(client.read(path).value)):
                        initialized = True
                else:
                    # Initialized, just wait
                    self.logger.debug("[%s] Watching config at path [%s]", self.Type, path)
                    self.update(json.loads(client.read(path, wait = True).value))
            except (EtcdKeyNotFound, EtcdWatchTimedOut, EtcdEventIndexCleared):
                # A normal error
                time.sleep(10)
            except:
                # Error, wait 30s and continue watch
                self.logger.exception("[%s] Failed to watch etcd, will retry in 30s", self.Type)
                time.sleep(30)

    def getInitialUpdatedValue(self):
        """Get updated value
        """
        if self._repository.etcd is None:
            raise ValueError("No etcd available")
        if self._environ:
            path = self._environ.getEtcdPath(self._key)
        else:
            path = self._repository.environ.getEtcdPath(self._key)
        # Get value
        return json.loads(self._repository.etcd.read(path).value)

    def update(self, value):
        """Update the config
        Parameters:
            value                           The config value
        Returns:
            Nothing
        """
        if not isinstance(value, dict):
            self.logger.error("[%s] Failed to update config, value must be a dict", self.Type)
            return False
        # Validate
        try:
            self.validate(value)
        except:
            self.logger.exception("[%s] Failed to validate config value: [%s]", self.Type, json.dumps(value, ensure_ascii = False))
            return False
        # Remove all values from self and update new values
        def updateConfig():
            """Update the config
            """
            self.clear()
            super(ConfigSection, self).update(value)
            self._timestamp = time.time()
        # Update
        if self._reloadLock:
            with self._reloadLock:
                updateConfig()
        else:
            updateConfig()
        # If reload is required
        if self.ReloadRequired:
            self._reloadEvent.set()
        # Updated
        self._updatedEvent.set()
        if self.logger.isEnabledFor(logging.DEBUG):
            self.logger.debug("[%s] Config updated [%s]", self.Type, json.dumps(value, ensure_ascii = False))
        # Done
        return True

    def validate(self, value):
        """Validate the config value
        """
        pass

    def __reload__(self):
        """Reload this config
        """
        self.logger.debug("[%s] Reload thread started", self.Type)
        reloadedTimestamp = 0.0
        while True:
            if reloadedTimestamp >= self._timestamp:
                # Wait for reload event
                self._reloadEvent.wait()
                self._reloadEvent.clear()
            # Start reload until reload succeed
            while True:
                try:
                    # Lock the reload lock, copy timestamp and config
                    with self._reloadLock:
                        reloadedTimestamp = self._timestamp
                        config = dict(self)
                    # Run reload
                    self.reload(config)
                except:
                    self.logger.exception("[%s] Failed to reload config, will retry in 30s", self.Type)
                    time.sleep(30)
                else:
                    self._reloadedEvent.set()
                    break

    def reload(self, config):
        """Reload this config
        """
        pass

    def close(self):
        """Close the config
        """
        pass

class ReferConfigSection(ConfigSection):
    """The config section which support reference counter
    """
    def reference(self, config):
        """Get the current referenced value
        """
        raise NotImplementedError

    def release(self, value):
        """The reference count has decreased to zero, could be released
        """
        pass

    def withinError(self, error):
        """When error occurred in the with statements
        """
        pass

    def getInstanceValue(self, value):
        """Get the value returned by instance method
        """
        yield value

    def reload(self, config):
        """Reload this section
        """
        self._value = ReferencedValue(self.reference(config))

    @contextmanager
    def instance(self):
        """Get the instance
        """
        value = None
        try:
            value = self._value
            # Increase
            value.increase()
            # Yield return the referenced value
            yield self.getInstanceValue(value.value).next()
        except Exception as error:
            # Run within error
            self.withinError(error)
            # Re-raise
            raise
        finally:
            if not value is None:
                # Decrease
                value.decrease()
            # Check if the referenced value is changed
            if self._value != value and value.notReferenced:
                # Updated and should be released
                self.logger.info("[%s] Referenced value changed and release is required", self.Type)
                # Release
                try:
                    self.release(value.value)
                except:
                    self.logger.exception('[%s] Failed to release the old referenced value', self.Type)

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
