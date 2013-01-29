"""
The configuration of sources, sink and channels uses INI format. There is one
root, or primary, configuration and zero or more included, or secondary,
configurations.

There are three settings sections:

    [general]
    # directory path for state storage, defaults to /tmp
    state_dir={string}

    # whether to enable (true) or disable (false) tracking, defaults to true
    tracking={flag}

    # timeout in seconds to wait before acquiring tracking lock, defaults to 15.0
    tracking-timeout={float}

    # other configuration files to include, default to none
    includes={comma separated file globs}

    [parse]
    # whether to fail-on (true) or ignore (false) malformed blocks, defaults to
    # true
    strict={flag}

    # size of read buffer in bytes when parsing for blocks, defaults to 2048
    read_size={integer}

    [throttle]
    # maximum number of seconds to throttle a sink, defaults to 600.0
    max={float}

    # base duration of sink throttle in seconds, defaults to 5.0
    duration={float}

    # threshold of sink throttle in seconds, 0 for no throttle, defaults to
    # 10.0
    latency={float}

    # percentage of duration to randomly deviate throttle, defaults to 2.0
    deviation={float}

    # whether to use exponential backoff on back-to-back throttles, defaults
    # to true
    backoff={flag}

Source sections, where the name of the source is prefixed by "source:":

    [source:{string}]  # {string} is the source name
    # which files to match as regexes, defaults to none
    patterns={comma separated file regexes}

    # which files to exclude as regexes, defaults to none
    exclude_patterns={comma separated file regexes}

    # which files to match as file globs, defaults to none
    globs={comma separated file globs}

    # which files to exclude as file globs, defaults to none
    exclude_globs={comma separated file globs}

    # delimiter for beginning of a block, defaults to none
    preamble={regex}

    # delimiter for ending of a block, defaults to \n
    terminal={string}

And channel section where the name of the source is prefixed by "channel:":

    [channel:{string}]  # {string} is the channel name
    # sources to associate with channel, required
    sources={comma separated source names}

    # sink specification as {type}[:argument], required
    sink={string}

    # flag indicating whether to backfill on source file discovery, defaults
    # to true
    backfill={flag}

    # maximum number of blocks to submit to a sink at once, defaults to 1000
    batch_size={integer}

    # overrides `parse`.`strict`, defaults to `parse`.`strict`
    parse_strict={flag}

    # overrides `parse`.`read_size`, defaults to `parse`.`read_size`
    parse_read_size={integer}

    # overrides `throttle_`.`max`, defaults to `throttle_`.`max`
    throttle_max={float}

    # overrides `throttle_`.`duration`, defaults to `throttle_`.`duration`
    throttle_duration={float}

    # overrides `throttle_`.`latency`, defaults to `throttle_`.`latency`
    throttle_latency={float}

    # overrides `throttle_`.`deviation`, defaults to `throttle_`.`deviation`
    throttle_deviation={float}

    # overrides `throttle_`.`backoff`, defaults to `throttle_`.`backoff`
    throttle_backoff={flag}

The root configuration can have all of these sections.

Included configurations (i.e. `general`:`includes`) can have all sections
**except** `general`.

Note that the `parse` and `throttle` sections of included configurations will
**only** affect sources and channels in that file. Fields of `parse` or
`throttle` not specified in an included configuration are inherited from root.
"""
from ConfigParser import SafeConfigParser
import fnmatch
import glob
import os
import re
import tempfile

from . import sink


def load(path, includes=None, excludes=None):
    """
    Loads configuration from `path`.

    :param path: Path to file from which to load configuration.
    :param includes: Names of channels to explicitly include. If None then all
                     channels are included. Defaults to None.
    :param excludes: Names of channels to explicitly exclude. If None then no
                     channel is excluded unless `includes` are provided and it
                     is not present.

    :return: Dictionary with loaded configuration information:

             {
                 "state_dir": string,
                 "tracking": bool,
                 "tracking_file": string,
                 "tracking_timeout": float,
                 "channels": [
                     {
                          "name": ,
                          "sources": [
                              {
                              "name": string,
                              "patterns": [regex, ...],
                              "preamble": regex,
                              "terminal": string,
                              }
                              ...
                              ],
                          "sink": (string, string),
                          "batch_size": int,
                          "backfill": flag,
                          "tag": string,
                          "parse_strict": bool,
                          "parse_read_size": int,
                          "throttle_max": float,
                          "throttle_duration": float,
                          "throttle_latency": float,
                          "throttle_deviation": v,
                          "throttle_backoff": bool,
                      }
                     ...
                 ]
             }
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
            conf['tracking_timeout'] = 15.0

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
    r['tracking_timeout'] = p.float('tracking-timeout')
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
    r['exclude_patterns'] = p.regexs('exclude_patterns', [])
    r['exclude_patterns'].extend(p.globs('exclude_globs', []))
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
    r['batch_size'] = p.int('batch_size', 1000)
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
            raise _InvalidFieldError(self, name, 'Not a float')

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
