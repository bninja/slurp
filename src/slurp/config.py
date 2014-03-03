"""
Configuration for managing all settings and creating:

    - sources
    - sinks
    - channels

based on those settings. Typically you will have settings stored in a file like
/etc/slurp/slurp.conf that may look like:

    [slurp]
    state_dir = /var/lib/slurp
    backfill = false
    strict = false
    read_size = 1024
    buffer_size = 1048576
    includes = /etc/slurp/conf.d/*.conf /etc/slurp/conf.d/*.py

You can then load these settings like:

.. code::

    import slurp

    config = slurp.Config.from_file('/etc/slurp/slurp.conf', 'slurp')

    print config.source_names
    print config.sink_names
    print config.channel_names

This will pickup additional configurations from:

    - /etc/slurp/conf.d/*.conf

and extensions (e.g. code forms, filters, sinks, etc) from:

    - /etc/slurp/conf.d/*.py

Now create and use, e.g. a channel like:

.. code::

    channel = config.channel(config.channel_names[0])
    channel.consume('/my/log/file')

"""
from ConfigParser import ConfigParser
import glob
import imp
import logging
import os
import re

from . import (
    settings,
    Settings,
    SourceSettings, Source,
    SinkSettings,
    ChannelSettings, Channel,
)


logger = logging.getLogger(__name__)


class GlobalSettings(Settings):

    #: Where to store slurp state (e.g. channel tracking files, etc).
    state_dir = settings.String(default=None).translate({'-': None})

    @state_dir.validate
    def state_dir(self, value):
        if value:
            if not os.path.isdir(value):
                self.ctx.errors.invalid('"{0}" does not exist'.format(value))
                return False
        return True

    #: Newrelic file.
    newrelic_file = settings.String(default='newrelic.ini')

    #: Newrelic environment.
    newrelic_env = settings.String(default=None)

    #: Default track flag.
    track = settings.Boolean(default=False)

    #: Default backfill flag.
    backfill = settings.Boolean(default=False)

    #: Default strict flag.
    strict = settings.Boolean(default=False)

    #: Default strict slack count.
    strict_slack = settings.Integer(default=0).min(0)

    #: Default source read size in bytes.
    read_size = settings.Integer(default=1024 * 4).min(1024)

    #: Default source read buffer size in bytes.
    buffer_size = settings.Integer(default=1024 * 1024).min(1024)

    @buffer_size.validate
    def buffer_size(self, value):
        if value < self.read_size:
            self.ctx.errors.invalid(
                'buffer_size {0} must be >= read_size {1}'.format(value, self.read_size),
            )
            return False
        return True

    #: Default batch count.
    batch_size = settings.Integer(default=100).min(1)

    #: File globs for other configurations (i.e. *.conf) or extensions
    #: (i.e. *.py) to load.
    includes = settings.List(settings.Glob(), default=[])


class Config(object):
    """
    Manages all settings and creation of:

        - sources
        - sinks
        - channels

    e.g.:

    .. code::

        import slurp

        config = slurp.Config.from_file('/etc/slurp/slurp.conf', 'slurp')

    """

    @classmethod
    def from_file(cls, path, section='slurp'):
        logger.info('loading config from "%s" section "%s"', path, section)
        return cls(**GlobalSettings.from_file(path, section))

    def __init__(self,
            includes=None,
            state_dir=GlobalSettings.state_dir.default,
            newrelic_file=GlobalSettings.newrelic_file.default,
            newrelic_env=GlobalSettings.newrelic_env.default,
            track=GlobalSettings.track.default,
            backfill=GlobalSettings.backfill.default,
            strict=GlobalSettings.strict.default,
            strict_slack=GlobalSettings.strict_slack.default,
            read_size=GlobalSettings.read_size.default,
            buffer_size=GlobalSettings.buffer_size.default,
            batch_size=GlobalSettings.batch_size.default,
        ):
        # globals
        self.state_dir = state_dir
        self.backfill = backfill
        self.strict = strict
        self.read_size = read_size
        self.buffer_size = buffer_size
        self.newrelic_file = newrelic_file
        self.newrelic_env = newrelic_env

        # ext
        from slurp import ext
        self.builtin_ext = ext

        # index
        self.confs = {}
        self.exts = {}
        self.sources = {}
        self.sinks = {}
        self.channels = {}

        # defaults
        self.sources_defaults = {
            'strict': strict,
            'read_size': read_size,
            'buffer_size': buffer_size,
        }
        self.channel_defaults = {
            'strict': strict,
            'strict_slack': strict_slack,
            'track': track,
            'backfill': backfill,
            'batch_size': batch_size,
        }

        # scan
        if includes is None:
            includes = []
        if isinstance(includes, basestring):
            includes = [includes]
        self._includes(includes)

    @property
    def source_names(self):
        """
        Names of all sources.

        .. code::

            sources = map(config.source, config.source_names)

        """
        return self.sources.keys()

    def source_settings(self, name, **overrides):
        """
        Loads `SourceSettings` instance for a named source.
        """
        path, section = self.sources[name]
        with settings.ctx(config=self):
            source_settings = SourceSettings.from_file(path, section)
        for name, value in self.sources_defaults.iteritems():
            if name in source_settings and source_settings[name] is None:
                source_settings[name] = value
        source_settings.update(**overrides)
        return source_settings

    def source(self, name, **overrides):
        """
        Loads `Source` instance for a name source.
        """
        return Source(name=name, **self.source_settings(name, **overrides))

    @property
    def sink_names(self):
        """
        Names of all sinks.
        """
        return self.sinks.keys()

    def sink_settings(self, name):
        """
        Loads `SinkSettings` instance for a named sink.
        """
        path, section = self.sinks[name]
        with settings.ctx(config=self):
            sink_type = SinkSettings.from_file(path, section).type
            return sink_type, sink_type.settings.from_file(path, section)

    def sink(self, name):
        """
        Loads `Sink` instance for a named sink.
        """
        sink_type, sink_settings = self.sink_settings(name)
        return sink_type(name=name, **sink_settings)

    @property
    def channel_names(self):
        """
        Names of all channels.
        """
        return self.channels.keys()

    def channel_settings(self, name, **overrides):
        """
        Loads `ChannelSettings` instance for a named channel.
        """
        path, section = self.channels[name]
        with settings.ctx(config=self):
            channel_settings = ChannelSettings.from_file(path, section)
        for name, value in self.channel_defaults.iteritems():
            if name in channel_settings and channel_settings[name] is None:
                channel_settings[name] = value
        channel_settings.update(overrides)
        return channel_settings

    def channel(self, name, **overrides):
        """
        Loads `Channel` instance for a named channel.
        """
        cs = self.channel_settings(name, **overrides)
        sources = cs.pop('sources')
        channel = Channel(
            name=name,
            state_dir=self.state_dir,
            **cs
        )
        for name, source in sources:
            channel.add_source(name=name, **source)
        return channel

    # internals

    def _includes(self, globs):
        for pattern in globs:
            logger.info('globbing "%s"', pattern)
            count = 0
            for path in glob.glob(pattern):
                if self._conf_re.match(path):
                    self._conf(path)
                    count += 1
                    continue
                if self._ext_re.match(path):
                    self._ext(path)
                    count += 1
                    continue
                logger.debug(
                    'include "%s" does not match "%s" or "%s", skipping ...',
                    path, self._conf_re.pattern, self._ext_re.pattern
                )
            if not count:
                logger.info('globbing "%s" found nothing', pattern)

    _conf_re = re.compile(r'.+?\.conf$')

    def _conf(self, path):
        logger.info('found conf "%s"', path)
        parser = ConfigParser()
        with open(path, 'r') as fp:
            parser.readfp(fp)
            for section in parser.sections():
                m = self._conf_section_re.match(section)
                if not m:
                    logger.info(
                        '"%s" has unknown section [%s], skipping ... ',
                        path, section
                    )
                    continue
                {
                    'source': self._conf_source,
                    'sink': self._conf_sink,
                    'channel': self._conf_channel,
                }[m.group('type')](path, section, m.group('name'))

    _conf_section_re = re.compile(r'^(?P<type>sink|source|channel):(?P<name>.+?)$')

    def _conf_sink(self, path, section, name):
        if name in self.sinks:
            raise ValueError('sink "%s" already loaded from "%s"', name, path)
        logger.debug('found sink %s @ "%s" section %s', name, path, section)
        self.sinks[name] = (path, section)

    def _conf_source(self, path, section, name):
        if name in self.sources:
            raise ValueError('source "%s" already loaded from "%s"', name, path)
        logger.debug('found source %s @ "%s" section %s', name, path, section)
        self.sources[name] = (path, section)

    def _conf_channel(self, path, section, name):
        if name in self.channels:
            raise ValueError('channel "%s" already loaded from "%s"', name, path)
        logger.debug('found channel %s @ "%s" section %s', name, path, section)
        self.channels[name] = (path, section)

    _ext_re = re.compile(r'.+\.py$')

    def _ext(self, path):
        name = os.path.splitext(os.path.basename(path))[0]
        logger.info('found extension %s @ "%s"', name, path)
        try:
            module = imp.load_source(name, path)
            self.exts[name] = module
        except Exception:
            logger.exception(
                'unable to load extension %s from "%s" skipping\n',
                name, path
            )
            self.exts[name] = None
