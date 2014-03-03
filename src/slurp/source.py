"""
A source defines a category of `Block` file and how to structure individual
blocks within it. To do that you say how to:

    - delimit a block (either a terminal or a prefix regex and a terminal)
    - extract named text fields from a block using a regex to a dict
    - optionally map the extract dict to something more type-ful

And that's a source. Here's how you might describe a line-oriented block file
where blocks represent HTTP accesses to some service:

.. code::

    import slurp

    class ServiceAccess(slurp.Form):

        ip = slurp.form.String()
        user = slurp.form.String(default=None)
        timestamp = slurp.form.Datetime(format='DD/MMM/YYYY:HH:mm:ss')
        method = slurp.form.String(default=None)
        uri = slurp.form.String(default=None)
        version = slurp.form.String(default=None)
        status = slurp.form.Integer(default=None)
        bytes = slurp.form.Integer(default=0)


    settings = slurp.SourceSettings(
        globs=['*/some-access', 'some-access'],
        terminal='\n',
        strict=True,
        pattern='''
(?P<ip>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\s+
-\s+
(?:(?P<user>\w+)|-)\s+
\[(?P<timestamp>\d{2}\/\w{3}\/\d{4}:\d{2}:\d{2}:\d{2})\]\s+
"(?:(?P<method>\w+)\s+(?P<uri>.+)\s+HTTP\/(?P<version>\d\.\d)|-)"\s+
(?:(?P<status>\d+)|-)\s+
(?:(?P<bytes>\d+)|-)\s+
"(?:(?P<referrer>.*?)|-)"\s+
"(?P<user_agent>.*?)"
        ''',
        form=ServiceAccess,
    )

    source = slurp.Source('my-service-source', **settings)


and now you can use it like:

.. code::

    from StringIO import StringIO
    from pprint import pprint

    path = '/tmp/logs/some-access'
    assert source.match(path)
    fo = StringIO('''\
1.2.3.1 - - [20/Feb/2014:16:57:20] "POST /customers HTTP/1.1" 201 1571 "-" "balanced-python/1.0.1beta2"
1.2.3.2 - - [20/Feb/2014:16:57:20] "POST /customers HTTP/1.1" 201 1571 "-" "balanced-python/1.0.1beta2"
1.2.3.3 - - [20/Feb/2014:16:57:20] "POST /customers HTTP/1.1" 201 1571 "-" "balanced-python/1.0.1beta2"
    ''')
    pprint(list(source.block(fo)))
    fo.seek(0)
    pprint(list(source.forms(fo)))

"""
import fnmatch
import logging
import re

from . import settings, Settings, form, Form, Blocks


logger = logging.getLogger(__name__)


class SourceSettings(Settings):

    #: Glob patterns used to determine whether a path is associated with
    #: (i.e. matches) this source.
    globs = settings.List(settings.Glob())

    #: Regex string. Note that regex strings are treated as verbose
    #: (http:/docs.python.org/2/library/re.html#re.X) and prefixed blocks will
    #: match newlines (http://docs.python.org/2/library/re.html#re.DOTALL).
    pattern = settings.String()

    @pattern.parse
    def pattern(self, value):
        parsed = self.ctx.field._parse(value)
        if parsed in form.IGNORE:
            return parsed
        flags = re.VERBOSE
        if self.prefix:
            flags |= re.DOTALL
        try:
            return re.compile(parsed, flags)
        except re.error, ex:
            self.ctx.errors.invalid(str(ex))
            return form.ERROR

    #: A module:attribute string or in-line code block that resolve to a
    #: call-able with this signature:
    #:
    #: ..code::
    #:
    #:      def filter(form, block):
    #:          return True
    #:
    #: If the call-able returns True the block is processed otherwise it is
    #: discarded.
    filter = settings.Code(default=None).as_callable(lambda form, block: None)

    #: A module:attribute string or in-line code that resolves to a
    #: :class:`Form`. This is used to map blocks.
    form = settings.Code(default=None).as_class(Form)

    #: A regex string that determines the start of a :class:`Block`. This is
    #: only needed if blocks cannot be unambiguously delimited by a terminal.
    prefix = settings.Pattern(default=None)

    #: A literal string that determines the end of a :class:`Block`.
    terminal = settings.String(default='\n')

    #: Flag indicating whether source block parsing must succeeded. If False
    #: block parse error a logged and the block skipped.
    strict = settings.Boolean(default=None)

    #: Size of block reads in bytes.
    read_size = settings.Integer(default=None)

    #: Size of unparsed block buffer in bytes.
    buffer_size = settings.Integer(default=None)

    @buffer_size.validate
    def buffer_size(self, value):
        if value < self.read_size:
            self.ctx.errors.invalid(
                self.ctx.field,
                'buffer_size {0} must be >= read_size {1}'.format(value, self.read_size),
            )
            return False
        return True


class Source(object):
    """
    A source defines a category of `Block` files and how to map block within
    those files into something structured (e.g. a dict).
    """

    def __init__(self,
            name,
            globs,
            pattern,
            form=None,
            filter=None,
            terminal='\n',
            prefix=None,
            strict=False,
            read_size=1024,
            buffer_size=1048576,
        ):
        self.name = name
        self.globs = []
        if isinstance(globs, basestring):
            globs = [globs]
        for glob in globs:
            if isinstance(glob, basestring):
                glob = re.compile(fnmatch.translate(glob))
            self.globs.append(glob)
        self.form = form
        self.filter = filter
        if isinstance(pattern, basestring):
            pattern = re.compile(pattern)
        self.pattern = pattern
        self.prefix = prefix
        self.terminal = terminal
        self.strict = strict
        self.read_size = read_size
        self.buffer_size = buffer_size

    def blocks(self, fo):
        """
        Creates a block iterator for a file-like object.
        """
        return Blocks(
            fo=fo,
            strict=self.strict,
            prefix=self.prefix,
            terminal=self.terminal,
            read_size=self.read_size,
            max_buffer_size=self.buffer_size,
        )

    def forms(self, fo):
        """
        Generator for blocks extracted from a file-like object.
        """
        path = getattr(fo, 'name', '<memory>')
        for block in self.blocks(fo):
            match = self.pattern.match(block.raw)
            if not match:
                if self.strict:
                    raise ValueError(
                        '{0} {1} @ {2} - does not match pattern {3}'.format(
                             self.name, path, block, self.pattern.pattern
                    ))
                logger.info(
                    '%s %s @ %s - does not match pattern "%s"',
                    self.name, path, block, self.pattern.pattern
                )
                continue
            f = dict(
                (k, str(v)) for k, v in match.groupdict().iteritems() if v is not None
            )
            if self.form:
                with form.ctx(block=block):
                    src = f
                    f = self.form()
                    errors = f(src)
                    if errors:
                        if self.strict:
                            raise ValueError('{0} {1} @ {2} - {3}'.format(
                                self.name, path, block, errors[0]
                            ))
                        logger.info(
                            '%s %s @ %s - %s', self.name, path, block, errors[0]
                        )
                        continue
                    f = f.filter('exclude', inv=True)
            if self.filter and not self.filter(f, block):
                continue
            yield f, block

    def match(self, path):
        """
        Determines whether a path is associated with this source.
        """
        for glob in self.globs:
            if glob.match(path):
                return True
        return False
