"""
"""
import fnmatch
import logging
import re

from . import settings, form
from .block import Blocks


logger = logging.getLogger(__name__)


class SourceSettings(settings.Form):

    #:
    glob = settings.Glob()

    #:
    pattern = settings.Code()

    #:
    form = settings.Code(default=None)

    #:
    prefix = settings.Pattern(default=None)

    #:
    terminal = settings.String(default='\n')

    #:
    strict = settings.Boolean(default=False)

    #:
    read_size = settings.Integer(default=4096)

    #:
    buffer_size = settings.Integer(default=1048576)



class Source(object):
    """
    """

    def __init__(self,
            name,
            glob,
            pattern,
            terminal=None,
            prefix=None,
            form=None,
            backfill=None,
            strict=None,
            read_size=None,
            buffer_size=None,
        ):
        self.name = name
        if isinstance(glob, basestring):
            glob = re.compile(fnmatch.translate(glob))
        self.glob = glob
        self.form = form
        if isinstance(pattern, basestring):
            pattern = re.compile(pattern)
        self.pattern = pattern
        self.prefix = prefix or SourceSettings.prefix.default
        self.terminal = terminal or SourceSettings.terminal.default
        self.strict = SourceSettings.strict.default if strict is None else strict
        self.read_size = SourceSettings.read_size.default if read_size is None else read_size
        self.buffer_size = SourceSettings.buffer_size.default if buffer_size is None else buffer_size

    def blocks(self, fo):
        return Blocks(
            fo=fo,
            strict=self.strict,
            prefix=self.prefix,
            terminal=self.terminal,
            read_size=self.read_size,
            max_buffer_size=self.buffer_size,
        )

    def forms(self, fo):
        for raw, offset in self.blocks(fo):
            match = self.pattern.match(raw)
            if not match:
                if self.strict:
                    raise ValueError()
                logger.warning('%s %s @ %s - does not match pattern', self.name, fo.name, offset)
                continue
            src = dict(
                (k, str(v)) for k, v in match.groupdict().iteritems() if v is not None
            )
            with form.ctx(path=fo.name, offset=offset):
                f = self.form()
                errors = f(src)
                if errors:
                    if self.strict:
                        raise ValueError()
                    logger.warning('%s %s @ %s - %s', self.name, fo.name, offset, errors[0])
                    continue
                f = f.filter('exclude', inv=True)
                yield f, offset
    
    def match(self, path):
        return self.glob.match(path) is not None
