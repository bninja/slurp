"""
"""
from ConfigParser import SafeConfigParser
import fnmatch
import glob
import os
import re
import tempfile

import sink


def load(path, includes=None, excludes=None):
    """
    """
    if not os.path.isfile(path):
        raise ValueError('Cannot find file "{0}"'.format(path))

    conf = {
        'channels': []
        }

    ctx = _Context(includes, excludes)

    with ctx(path):
        ctx.general = _load_general(ctx)
        conf['state_dir'] = ctx.general['state_dir']
        conf['tracking'] = ctx.general['tracking']
        if conf['tracking']:
            conf['tracking_file'] = os.path.join(conf['state_dir'], 'tracking.db')

        ctx.parse = _load_parse(ctx)

        ctx.throttle = _load_throttle(ctx)

        ctx.sources = {}
        for section, name in _sections(ctx.parser, 'source:'):
            source = _load_source(ctx, section, name)
            ctx.sources[source['name']] = source

        channels = []
        for section, name in _sections(ctx.parser, 'channel:'):
            if not ctx.channel_filter(name):
                continue
            channel = _load_channel(ctx, section, name)
            channels.append(channel)
        conf['channels'].extend(channels)

        for include in ctx.general['includes']:
            for file_path in _files(include):
                with ctx(file_path):
                    ctx.parse = _load_parse(ctx)
                    ctx.throttle = _load_throttle(ctx)

                    ctx.sources = {}
                    for section, name in _sections(ctx.parser, 'source:'):
                        source = _load_source(ctx, section, name)
                        ctx.sources[source['name']] = source

                    channels = []
                    for section, name in _sections(ctx.parser, 'channel:'):
                        if not ctx.channel_filter(name):
                            continue
                        channel = _load_channel(ctx, section, name)
                        channels.append(channel)
                    conf['channels'].extend(channels)

    return conf


def _files(path):
    paths = glob.glob(path)
    for path in paths:
        if os.path.isfile(path):
            yield path
        else:
            for dir_name, dir_names, file_names in os.walk(path):
                for file_name in file_names:
                    file_path = os.path.join(dir_name, file_name)
                    if os.path.isfile(file_path):
                        yield file_path


def _sections(section_parser, prefix):
    for section in section_parser.sections():
        if not section.startswith(prefix):
            continue
        name = section[len(prefix):]
        yield section, name


class _Context(object):

    class _Frame(object):
        pass

    def __init__(self, includes, excludes):
        self._stack = []
        self.channel_filter = _IncludeExcludeFilter(includes, excludes)
        self.defaults = {
            'general': {
                'tracking': True,
                'state_dir': tempfile.gettempdir(),
                },
            'parse': {
                'strict': True,
                'read_size': 2048,
                },
            'throttle': {
                'max': 600.0,
                'duration': 5.0,
                'latency': 10.0,
                'deviation': 2.0,
                'backoff': True,
                },
            'source': {
                'termainal': '\n',
                },
            'channel': {
                'backfill': False,
                },
            }

    @property
    def depth(self):
        return len(self._stack)

    def default(self, section, name):
        if section.startswith('source:'):
            defaults = getattr(self, 'source', {})
        elif section.startswith('channel:'):
            defaults = getattr(self, 'channel', {})
        else:
            defaults = getattr(self, section, {})
        return defaults.get(name, _NONE)

    def __call__(self, path):
        self._stack.append(self._Frame())
        self.path = path
        self.parser = SafeConfigParser()
        self.parser.read(path)
        if self.depth == 1:
            for k, v in self.defaults.iteritems():
                setattr(self, k, v)
        return self

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self._stack.pop()

    def __getattr__(self, name):
        for frame in reversed(self._stack):
            if hasattr(frame, name):
                return getattr(frame, name)
        raise AttributeError(
            '"{}" object has no attribute "{}"'.format(
            self.__class__.__name__, name))

    def __setattr__(self, name, value):
        if name not in ('_stack'):
            if self._stack:
                return setattr(self._stack[-1], name, value)
        return super(_Context, self).__setattr__(name, value)


def _load_general(ctx):
    r = {}
    p = _SectionParser(ctx, 'general')
    r['state_dir'] = p.string('state_dir')
    r['tracking'] = p.bool('tracking')
    r['includes'] = p.strings('includes', [])
    return r


def _load_parse(ctx):
    r = {}
    p = _SectionParser(ctx, 'parse')
    r['strict'] = p.bool('strict')
    r['read_size'] = p.int('read_size')
    return r


def _load_throttle(ctx):
    r = {}
    p = _SectionParser(ctx, 'throttle')
    r['max'] = p.float('max')
    r['duration'] = p.float('duration')
    r['latency'] = p.float('latency')
    r['deviation'] = p.float('deviation')
    r['backoff'] = p.bool('backoff')
    return r


def _load_source(ctx, section, name):
    r = {}
    r['name'] = name
    p = _SectionParser(ctx, section)
    r['patterns'] = p.regexs('patterns', [])
    r['patterns'].extend(p.globs('globs', []))
    r['preamble'] = p.regex('preamble', None)
    r['terminal'] = p.string('terminal').decode('string_escape')
    return r


def _load_channel(ctx, section, name):
    r = {}
    r['name'] = name
    p = _SectionParser(ctx, section)
    sources = p.strings('sources')
    r['sources'] = []
    for source in sources:
        if source not in ctx.sources:
            raise _InvalidFieldError(
                p, 'sources', 'Unknown source "{}"'.format(source))
        r['sources'].append(ctx.sources[source])
    r['sink'] = p.sink('sink')
    r['tag'] = p.string('tag', None)
    r['backfill'] = p.bool('backfill')
    r['throttle_max'] = p.float('throttle_max', ctx.throttle['max'])
    r['throttle_duration'] = p.float('throttle_duration', ctx.throttle['duration'])
    r['throttle_latency'] = p.float('throttle_latency', ctx.throttle['latency'])
    r['throttle_deviation'] = p.float('throttle_deviation', ctx.throttle['deviation'])
    r['throttle_backoff'] = p.bool('throttle_backoff', ctx.throttle['backoff'])
    r['parse_strict'] = p.bool('strict', ctx.parse['strict'])
    r['parse_read_size'] = p.int('read_size', ctx.parse['read_size'])
    return r


class _IncludeExcludeFilter(object):

    def __init__(self, includes, excludes):
        self.includes = includes
        self.excludes = excludes

    def __call__(self, name):
        included = True
        if self.includes:
            included = name in self.includes
        excluded = False
        if self.excludes:
            excluded = name in self.excludes
        return (included and not excluded)


_NONE = object()


class _SectionParser(object):

    def __init__(self, ctx, section):
        self.ctx = ctx
        self.section = section

    def _raw(self, name, default):
        if not self.ctx.parser.has_option(self.section, name):
            if default is _NONE:
                default = self.ctx.default(self.section, name)
                if default is _NONE:
                    raise _MissingFieldError(self, name)
            return default, True
        return self.ctx.parser.get(self.section, name), False

    def bool(self, name, default=_NONE):
        r, defaulted = self._raw(name, default)
        if defaulted:
            return r
        r = r.lower()
        if r in ('true', 't', '1'):
            return True
        if r in ('false', 'f', '0'):
            return False
        raise _InvalidFieldError(self, name, 'Not a boolean')

    def int(self, name, default=_NONE):
        r, defaulted = self._raw(name, default)
        if defaulted:
            return r
        try:
            return int(r)
        except TypeError:
            raise _InvalidFieldError(self, name, 'Not an integer')

    def float(self, name, default=_NONE):
        r, defaulted = self._raw(name, default)
        if defaulted:
            return r
        try:
            return float(r)
        except TypeError:
            raise _InvalidFieldError(self, name, 'Not an integer')

    def sink(self, name, default=_NONE):
        r, defaulted = self._raw(name, default)
        if defaulted:
            return r
        if r == 'null':
            r = 'file:/dev/null'
        elif r == 'stdout':
            r = 'file:/dev/stdout'
        type, _, arg = r.partition(':')
        if type not in sink.registry:
            raise _InvalidFieldError(
                self, name, 'Not a valid sink type "{0}"'.format(type))
        return (type, arg)

    def string(self, name, default=_NONE):
        r, defaulted = self._raw(name, default)
        return r

    def strings(self, name, default=_NONE):
        r, defaulted = self._raw(name, default)
        if defaulted:
            return r
        return r.split(', ')

    def regex(self, name, default=_NONE):
        r, defaulted = self._raw(name, default)
        if defaulted:
            return r
        try:
            return re.compile(r)
        except ValueError:
            raise _InvalidFieldError(self, name, 'Not a regex')

    def regexs(self, name, default=_NONE):
        r, defaulted = self._raw(name, default)
        if defaulted:
            return r
        r = r.split(', ')
        rs = []
        for i, p in enumerate(r):
            try:
                rs.append(re.compile(p))
            except ValueError:
                raise _InvalidFieldError(self, name, '[%s] is not a regex')
        return rs

    def globs(self, name, default=_NONE):
        r, defaulted = self._raw(name, default)
        if defaulted:
            return r
        r = r.split(', ')
        rs = []
        for i, p in enumerate(r):
            try:
                rs.append(re.compile(fnmatch.translate(p)))
            except ValueError:
                raise _InvalidFieldError(self, name, '[%s] is not a glob')
        return rs


class _InvalidFieldError(ValueError):

    def __init__(self, section_parser, name, msg):
        message = 'File "{0}" section [{1}] has invalid field "{2}" -- {3}'.format(
            section_parser.ctx.path, section_parser.section, name, msg)
        super(_InvalidFieldError, self).__init__(message)


class _MissingFieldError(ValueError):

    def __init__(self, section_parser, name):
        message = 'File "{0}" section [{1}] missing required "{2}"'.format(
            section_parser.ctx.path, section_parser.section, name)
        super(_MissingFieldError, self).__init__(message)

