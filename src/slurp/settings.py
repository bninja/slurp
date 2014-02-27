from ConfigParser import ConfigParser
import re
import shlex
import logging

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

ctx = pilo.ctx

class Form(pilo.Form):
    
    @classmethod
    def from_file(cls, path, section):
        config = ConfigParser()
        with open(path, 'r') as fo:
            config.readfp(fo)
        src = Source(config, section)
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

    def _parse(self, value):
        parsed = super(Pattern, self)._parse(value)
        return re.compile(parsed)
    
    
class Code(String):

    def _parse(self, value):
        # crack
        pattern = '(?:(?P<module>[\w\.]+):)?(?P<attr>[\w\.]+)'
        match = re.match(pattern, value)
        if not match:
            self.ctx.errors.invalid(
                self.ctx.field, 'does not match pattern "{0}"'.format(pattern)
            )
            return pilo.ERROR
        name = match.group('module')
        attr = match.group('attr')

        # module
        if name is None:
            module = self.ctx.config.builtin_ext
        elif name in self.ctx.config.exts:
            module = self.ctx.config.exts[name]
            if module is None:
                self.ctx.errors.invalid(
                    self.ctx.field, 'could not load extension {0}'.format(name)
                )
                return pilo.ERROR
        else:
            try:
                module = __import__(name)
            except Exception, ex:
                self.ctx.errors.invalid(
                    self.ctx.field,
                    'unable to import {0} - {1}\n'.format(module, ex)
                )
                return pilo.ERROR

        # attribute
        try:
            obj = reduce(getattr, attr.split('.'), module)
        except Exception, ex:
            self.ctx.errors.invalid(
                self.ctx.field,
                'unable to resolve {0} - {1}\n'.format(attr, ex)
            )
            return pilo.ERROR

        logger.debug('loaded %s from %s', obj, value)
        return obj


class Source(pilo.Source):  

    def __init__(self, config, section):
        super(Source, self).__init__()
        self.source = config
        self.section = section
        self.parsers = {
            basestring: self._as_string,
            bool: self._as_boolean,
            int: self._as_integer,
            float: self._as_float,
        }
        
    def path(self, key):
        if key in (None, pilo.NONE):
            path = pilo.ctx.src_path
        else:
            path = pilo.ctx.src_path + [key]
        return path

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
