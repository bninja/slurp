from ConfigParser import ConfigParser
import inspect
import logging
import re
import shlex

import pilo


__all__ = [
    'Form',
    'Integer',
    'Boolean',
    'List',
]

logger = logging.getLogger(__name__)

NONE = pilo.NONE

ERROR = pilo.ERROR

Error = pilo.FieldError

ctx = pilo.ctx

class Form(pilo.Form):

    @classmethod
    def from_file(cls, path, section):
        config = ConfigParser()
        with open(path, 'r') as fo:
            config.readfp(fo)
        src = Source(config, section, path)
        return cls(src)

Field = pilo.Field

Boolean = pilo.Field

Boolean = pilo.fields.Boolean

Integer = pilo.fields.Integer

List = pilo.fields.List

Dict = pilo.fields.Dict

String = pilo.fields.String


class Glob(pilo.fields.String):

    pass


class Pattern(pilo.Field):

    def __init__(self, *args, **kwargs):
        self.flags = kwargs.pop('flags', 0)
        super(Pattern, self).__init__(*args, **kwargs)

    def _parse(self, value):
        parsed = super(Pattern, self)._parse(value)
        try:
            return re.compile(parsed, self.flags)
        except re.error, ex:
            from ipdb import set_trace; set_trace()
            self.ctx.errors.invalid(str(ex))
            return ERROR


class Code(String):

    pattern = re.compile('(?:(?P<module>[\w\.]+):)?(?P<attr>[\w\.]+)')


    def as_callable(self, sig=None):
        arg_spec = inspect.getargspec(sig) if sig else None

        def validate(self, value):
            if not callable(value):
                self.ctx.errors.invalid(
                    self.ctx.field, '"{0}" is not callable'.format(value)
                )
            if arg_spec and not inspect.getargspec(value) != arg_spec:
                self.ctx.errors.invalid(
                    self.ctx.field,
                    '"{0}" does not match signature {1}'.format(value, arg_spec)
                )
                return False
            return True

    def as_class(self, *clses):

        def validate(self, value):
            if value:
                if not inspect.isclass(value):
                    self.ctx.errors.invalid(
                        self.ctx.field, '"{0}" is not a class'.format(value)
                    )
                    return False
                if not any(issubclass(value, cls) for cls in clses):
                    self.ctx.errors.invalid(
                        self.ctx.field,
                        '"{0}" is not a sub-class of {1}'.format(value, list(clses))
                    )
                    return False
            return True

        return self.validate.attach(self)(validate)

    @classmethod
    def match(cls, value):
        match = cls.pattern.match(value)
        if not match:
            return False
        return match.group('module'), match.group('attr')

    @classmethod
    def load(cls, name, attr):
        # module
        if name is None:
            module = ctx.config.builtin_ext
        elif name in ctx.config.exts:
            module = ctx.config.exts[name]
            if module is None:
                raise RuntimeError('Could not load extension {0}'.format(name))
        else:
            module = __import__(name)

        # attribute
        try:
            obj = reduce(getattr, attr.split('.'), module)
        except AttributeError:
            raise TypeError('Unable to resolve {0}.{1}\n'.format(
                module.__name__, attr
            ))
        logger.debug('loaded %s from %s.%s', obj, module.__name__, attr)
        return obj

    def _parse(self, value):
        # already
        if not isinstance(value, basestring):
            return value

        # crack
        match = self.match(value)
        if not match:
            self.ctx.errors.invalid(
                self.ctx.field, 'does not match pattern "{0}"'.format(self.pattern.pattern)
            )
            return pilo.ERROR
        name, attr = match

        # load
        try:
            return self.load(name, attr)
        except Exception, ex:
            name, attr = match
            self.ctx.errors.invalid(str(ex))
            return pilo.ERROR


class SourcePath(list):

    def __init__(self, src, *args, **kwargs):
        self.src = src
        super(SourcePath, self).__init__(*args, **kwargs)

    def __str__(self):
        parts = []
        if self.src.file_path:
            parts.append(self.src.file_path)
        parts.append('[{0}]'.format(self.src.section))
        field = ''.join(([self[0]] + ['[{0}]'.format(f) for f in self[1:]]))
        parts.append(field)
        return ' '.join(parts)



class Source(pilo.Source):

    def __init__(self, config, section, file_path=None):
        super(Source, self).__init__()
        self.source = config
        self.section = section
        self.file_path = file_path
        self.parsers = {
            basestring: self._as_string,
            bool: self._as_boolean,
            int: self._as_integer,
            float: self._as_float,
        }

    def path(self, key=None):
        src_path = getattr(pilo.ctx, 'src_path', None)
        if src_path is None:
            src_path = SourcePath(self)
        if key in (None, pilo.NONE):
            return src_path
        return SourcePath(self, src_path + [key])

    def resolve(self, key):
        path = self.path(key)

        # option
        if len(path) == 1:
            option = path[0]
            if self.source.has_option(self.section, option):
                return self.source.get(self.section, option)
            return pilo.NONE

        # container
        if len(path) == 2:
            option = '{0}[{1}]'.format(*path)
            if self.source.has_option(self.section, option):
                return self.source.get(self.section, option)
            return pilo.NONE

        return pilo.NONE

    def sequence(self, key):
        if not self.source.has_option(self.section, key):
            return pilo.NONE
        raw = self.source.get(self.section, key)
        count = 0
        for i, value in enumerate(shlex.split(raw)):
            self.source.set(self.section, '{0}[{1}]'.format(key, i), value)
            count += 1
        return count

    def mapping(self, key):
        pattern = key + '\[(\w+)\]'
        keys = []
        for option in self.source.options(self.section):
            m = re.match(pattern, option)
            if not m:
                continue
            keys.append(m.group(1))
        if not keys:
            return pilo.NONE
        return keys

    def parse(self, key, value, type):
        return self.parser_for(type)(key, value)
