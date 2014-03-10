from ConfigParser import ConfigParser
import inspect
import logging
import re
import textwrap

import pilo


__all__ = [
    'Form',
    'Integer',
    'Float',
    'Boolean',
    'List',
]

logger = logging.getLogger(__name__)

NONE = pilo.NONE

ERROR = pilo.ERROR

IGNORE = pilo.IGNORE

Error = pilo.FieldError

ctx = pilo.ctx


class Settings(pilo.Form):

    @classmethod
    def from_file(cls, path, section):
        config = ConfigParser()
        with open(path, 'r') as fo:
            config.readfp(fo)
        src = Source(config, section, path)
        with ctx(section=section):
            return cls(src)


Field = pilo.Field

Boolean = pilo.Field

Boolean = pilo.fields.Boolean

Integer = pilo.fields.Integer

Float = pilo.fields.Float

List = pilo.fields.List

Dict = pilo.fields.Dict

String = pilo.fields.String

Tuple = pilo.fields.Tuple

class Glob(pilo.fields.String):

    pass


class Pattern(pilo.Field):

    def __init__(self, *args, **kwargs):
        self.flags = kwargs.pop('flags', 0)
        super(Pattern, self).__init__(*args, **kwargs)

    def _parse(self, value):
        parsed = super(Pattern, self)._parse(value)
        if parsed in IGNORE:
            return parsed
        try:
            return re.compile(parsed, self.flags)
        except re.error, ex:
            self.ctx.errors.invalid(str(ex))
            return ERROR


class Code(String):

    pattern = re.compile('(?:(?P<module>[\w\.]+):)?(?P<attr>[\w\.]+)')

    def as_callable(self, sig=None):
        arg_spec = inspect.getargspec(sig) if sig else None

        def validate(self, value):
            if value:
                if not callable(value):
                    self.ctx.errors.invalid(
                        '"{0}" is not callable'.format(value)
                    )
                # TODO: compatibility check
#                if arg_spec and not inspect.getargspec(value) != arg_spec:
#                    self.ctx.errors.invalid(
#                        '"{0}" does not match signature {1}'.format(value, arg_spec)
#                    )
#                    return False
            return True

        return self.validate.attach(self)(validate)

    def as_class(self, *clses):

        def validate(self, value):
            if value:
                if not inspect.isclass(value):
                    self.ctx.errors.invalid(
                        '"{0}" is not a class'.format(value)
                    )
                    return False
                if not any(issubclass(value, cls) for cls in clses):
                    self.ctx.errors.invalid(
                        '"{0}" is not a sub-class of {1}'.format(value, list(clses))
                    )
                    return False
            return True

        return self.validate.attach(self)(validate)

    @classmethod
    def import_match(cls, value):
        match = cls.pattern.match(value)
        if not match:
            return False
        return match.group('module'), match.group('attr')

    @classmethod
    def inline_match(cls, value):
        return value.count('\n') > 0

    @classmethod
    def load(cls, name, attr):
        # module
        if name is None:
            module = ctx.config.builtin_ext
        elif name in ctx.config.exts:
            module = ctx.config.exts[name]
            if module is None:
                raise ValueError('Could not load extension {0}'.format(name))
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

    @classmethod
    def compile(cls, name, code, **code_globals):
        import slurp

        code_globals.update({
            'slurp': slurp,
            'logger': logger,
        })
        if hasattr(ctx, 'section'):
            code_globals['logger'] = logging.getLogger(
                'slurp.settings[{0}]'.format(ctx.section)
            )
        exec code in code_globals
        if name not in code_globals:
            raise TypeError('Code does not define a "{0}" attribute'.format(
                name
            ))
        return code_globals[name]

    def _parse(self, path):
        value = super(Code, self)._parse(path)
        if value in IGNORE:
            return value

        # in-line
        if self.inline_match(value):
            try:
                return self.compile(self.name, value)
            except Exception, ex:
                self.ctx.errors.invalid(str(ex))
                return pilo.ERROR

        # import
        match = self.import_match(value)
        if match:
            name, attr = match
            try:
                return self.load(name, attr)
            except Exception, ex:
                self.ctx.errors.invalid(str(ex))
                return pilo.ERROR

        self.ctx.errors.invalid('"{0}" does not match pattern "{1}" and it not a code block'.format(
            value, self.pattern.pattern
        ))
        return pilo.ERROR


class Source(pilo.source.ConfigSource):

    def as_raw(self, path):
        option = path[-1]
        lines = []
        with open(self.location, 'r') as fo:
            section_header = '[{}]'.format(self.section)
            for line in fo:
                if line.strip() == section_header:
                    break
            for line in fo:
                if line.strip().startswith(option):
                    break
            for line in fo:
                if line and not line[0].isspace():
                    break
                lines.append(line)
        return textwrap.dedent(''.join(lines))

    def primitive(self, path, type=None):
        value = super(Source, self).primitive(path, type)

        # HACK: preserve white-space for mulit-line strings
        if isinstance(value, basestring) and value.count('\n') > 0:
            value = self.as_raw(path)

        return value
