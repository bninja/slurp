import collections
import functools
import logging


logger = logging.getLogger(__name__)


BlockOffset = collections.namedtuple('BlockOffset', ['path', 'begin', 'end'])

class Blocks(object):
    """
    `fo`
        File-like object we are parsing for blocks.

    `strict`
        Flag indicating whether to fail (True) or ignore (False) malformed
        blocks. Defaults to False.

    `read_size`
        The number of bytes to read at a time. Defaults to 2048.

    `max_buffer_size`
        The maximum number of bytes to buffer at a time or None for maximum.
        Defaults to None.
    """

    def __init__(self,
            fo,
            strict=False,
            prefix=None,
            terminal='\n',
            read_size=2048,
            max_buffer_size=1048576
        ):
        self.fo = fo
        self.read_size = read_size
        self.max_buffer_size = max_buffer_size
        self.strict = strict
        if prefix is not None:
            self.iter_cls = functools.partial(
                MultiLineIterator, preamble=prefix, terminal=terminal
            )
        else:
            self.iter_cls = functools.partial(
                LineIterator, terminal=terminal
            )
        
    def __iter__(self):
        return self.iter_cls(
            self.fo,
            strict=self.strict,
            read_size=self.read_size,
            max_buffer_size=self.max_buffer_size
        )

    def tell(self):
        return self.fo.tell()

    def seek(self, offset, whence):
        return self.fo.seek(offset, whence)


class BlockIterator(object):
    """
    Base class for "block" parsers. A "block" within a file is just a delimited
    string. For log files these "blocks" are typically called entries. Derived
    classes need to determine how "blocks" are delimited.
    """

    def __init__(self, fo, strict=False, read_size=2048, max_buffer_size=1048576):
        self.fo = fo
        if hasattr(fo, 'isatty') and fo.isatty():
            self.pos = 0
        else:
            self.pos = fo.tell()
        self.strict = strict
        self.read_size = read_size
        self.max_buffer_size = max_buffer_size
        self.buf = bytearray()
        self.eof = False
        self.discard = False

    def __iter__(self):
        return self

    def next(self):
        while self.buf:
            result = self._parse(self.eof)
            if result:
                raw, offset_b, offset_e = result
                offset = BlockOffset(path=getattr(self.fo, 'name', '<memory>'), begin=offset_b, end=offset_e)
                return raw, offset
        while not self.eof:
            if self.max_buffer_size is None:
                buf = self.fo.read(self.read_size)
                self.eof = (len(buf) != self.read_size)
            else:
                read_size = min(self.read_size, self.max_buffer_size - len(self.buf))
                buf = self.fo.read(read_size)
                self.eof = (len(buf) != read_size)
            self.buf.extend(buf)
            result = self._parse(self.eof)
            if not result:
                if self.max_buffer_size is not None and len(self.buf) >= self.max_buffer_size:
                    if self.strict:
                        raise ValueError(
                            '{0}[{1}:{2}] partial block exceeds buffer size {3}'.format(
                            self.fo.name,
                            self.pos, self.pos + len(self.buf),
                            self.max_buffer_size
                        ))
                    logger.warning(
                        '%s[%s:%s] partial block exceeds buffer size %s, discarding',
                        self.fo.name,
                        self.pos, self.pos + len(self.buf),
                        self.max_buffer_size
                    )
                    self.discard = True
                    del self.buf[:]
                continue
            raw, offset_b, offset_e = result
            if self.discard:
                logger.info('%s[%s:%s] partial block, discarding', self.fo.name, offset_b, offset_e)
                self.discard = False
                continue
            offset = BlockOffset(path=getattr(self.fo, 'name', '<memory>'), begin=offset_b, end=offset_e)
            return raw, offset
        raise StopIteration()

    def _parse(self, eof):
        raise NotImplementedError()


class LineIterator(BlockIterator):
    """
    A block parser where all "blocks" are unambiguously delimited by a
    terminal string. Apache HTTP access logs are a good example of a line
    oriented block parser where the terminal is '\n'.

    `terminal`
        String used to delimit blocks.
    """

    def __init__(self, fo, terminal, **kwargs):
        super(LineIterator, self).__init__(fo, **kwargs)
        self.terminal = terminal

    def _parse(self, eof):
        index = self.buf.find(self.terminal)
        if index == -1:
            return None
        index += len(self.terminal)
        result = self.buf[:index], self.pos, self.pos + index
        self.buf = self.buf[index:]
        self.pos += index
        return result


class MultiLineIterator(BlockIterator):
    """
    A block parser where all "blocks" are delimited by a preamble (i.e. prefix)
    regex and a terminal string. Multi-line error logs are a good example of a
    multi-line oriented block parser.

    `preamble`
        Regex used to identify the beginning of a block.

    `terminal`
        String used to identify the end of a block.
    """

    def __init__(self, fo, preamble, terminal, **kwargs):
        super(MultiLineIterator, self).__init__(fo, **kwargs)
        self.preamble = preamble
        self.terminal = terminal

    def _parse(self, eof):
        match = self.preamble.search(self.buf)
        if not match:
            logger.debug('%s[%s:%s] has no preamble', self.fo.name,
                self.pos, self.pos + len(self.buf))
            return None
        if match.start() != 0:
            if self.strict:
                raise ValueError('%s[%s:%s] is partial block',
                    self.fo.name, self.pos, self.pos + match.start())
            logger.warning('%s[%s:%s] is partial block, discarding',
                self.fo.name, self.pos, self.pos + match.start())
            self.buf = self.buf[match.start():]
            self.pos += match.start()
        logger.debug('%s[%s:] has preamble', self.fo.name, self.pos)
        next = match
        while True:
            prev = next
            next = self.preamble.search(self.buf, prev.end())
            if not next:
                logger.debug('%s[%s:] contains no preamble',
                    self.fo.name, self.pos + prev.end())
                break
            prefix = self.buf[
                next.start() - len(self.terminal):next.start()]
            if prefix == self.terminal:
                logger.debug('%s[%s:] contains terminal-prefixed preamble',
                    self.fo.name, self.pos + next.end())
                break
            logger.debug('%s[%s:] contains non-terminal-prefixed preamble',
                self.fo.name, self.pos + next.end())
            return None
        if next:
            logger.debug('%s[%s:%s] hit', self.fo.name, self.pos,
                self.pos + next.start())
            raw = str(self.buf[:next.start()])
            self.buf = self.buf[next.start():]
        else:
            if not eof:
                return None
            suffix = self.buf[-len(self.terminal):]
            if suffix != self.terminal:
                return None
            logger.debug('%s[%s:] hit', self.fo.name, self.pos)
            raw = str(self.buf)
            del self.buf[:]
        result = raw, self.pos, self.pos + len(raw)
        self.pos += len(raw)
        return result
