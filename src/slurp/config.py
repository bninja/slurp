"""
"""
from ConfigParser import ConfigParser
import glob
import imp
import logging
import os
import re

from . import (
    settings,
    SourceSettings, Source,
    SinkSettings,
    ChannelSettings, Channel,
)


logger = logging.getLogger(__name__)


class GlobalSettings(settings.Form):

    #:
    state_dir = settings.String(default=None).translate({'-': None})
    
    @state_dir.validate
    def state_dir(self, value):
        if value:
            if not os.path.isdir(value):
                self.ctx.errors.invalid(
                    self.ctx.field, '"{0}" does not exist'.format(value)
                )
                return False
        return True

    #: 
    backfill = settings.Boolean(default=False)

    #: 
    strict = settings.Boolean(default=False)
    
    #: 
    read_size = settings.Integer(default=4096).min(0)
    
    #: 
    buffer_size = settings.Integer(default=1048576).min(0)
    
    @buffer_size.validate
    def buffer_size(self, value):
        if value < self.read_size:
            self.ctx.errors.invalid(
                'buffer_size {} must be >= read_size {}'.format(value, self.read_size),
            )
            return False
        return True
    
    #: 
    includes = settings.List(settings.Glob(), default=[])


class Config(object):
    """
    """

    @classmethod
    def from_file(cls, path, section='slurp'):
        logger.info('loading config from "%s" section "%s"', path, section)
        return cls(**GlobalSettings.from_file(path, section))
    
    def __init__(self,
            includes=None,
            state_dir=GlobalSettings.state_dir.default,
            backfill=GlobalSettings.backfill.default,
            strict=GlobalSettings.strict.default,
            read_size=GlobalSettings.read_size.default,
            buffer_size=GlobalSettings.buffer_size.default,
        ):
        # globals
        self.state_dir = state_dir
        self.backfill = backfill
        self.strict = strict
        self.read_size = read_size
        self.buffer_size = buffer_size
        
        # ext
        from slurp import ext
        self.builtin_ext = ext

        # index
        self.confs = {}
        self.exts = {}
        self.sources = {}
        self.sinks = {}
        self.channels = {}

        # scan
        if includes is None:
            includes = []
        if isinstance(includes, basestring):
            includes = [includes]
        self._includes(includes)
        
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
                logger.info(
                    'include "%s" does not match "%s" or "%s", skipping ...',
                    self._conf_re.pattern, self._ext_re.pattern
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

    @property
    def source_names(self):
        return self.sources.keys()
    
    def source_settings(self, name):
        path, section = self.sources[name]
        with settings.ctx(config=self):
            return SourceSettings.from_file(path, section)

    def source(self, name):
        return Source(name=name, **self.source_settings(name))

    @property
    def sink_names(self):
        return self.sinks.keys()
    
    def sink_settings(self, name):
        path, section = self.sinks[name]
        with settings.ctx(config=self):
            sink_type = SinkSettings.from_file(path, section).type
            return sink_type, sink_type.settings.from_file(path, section)

    def sink(self, name):
        sink_type, sink_settings = self.sink_settings(name)
        return sink_type(name=name, **sink_settings)
    
    @property
    def channel_names(self):
        return self.channels.keys()
    
    def channel_settings(self, name, **overrides):
        path, section = self.channels[name]
        with settings.ctx(config=self):
            cs = ChannelSettings.from_file(path, section)
            cs.update(overrides)
            return cs
    
    def channel(self, name, **overrides):
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
