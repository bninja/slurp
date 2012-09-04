import logging


logger = logging.getLogger(__name__)


class BlockIterator(object):
    """
    Base class for "block" parsers. A "block" within a file is just a delimited
    string. For log files these "blocks" are typically called entries. Derived
    classes need to determine how "blocks" are delimited.

    `fo`
        File-like object we are parsing for blocks.

    `strict`
        Flag indicating whether to fail (True) or ignore (False) malformed
        blocks. Defaults to False.

    `read_size`
        The number of bytes to read at a time. Defaults to 2048.
    """
    def __init__(self, fo, strict=False, read_size=2048):
        self.fo = fo
        self.pos = fo.tell()
        self.strict = strict
        self.read_size = read_size
        self.buffer = ''
        self.eof = False

    def __iter__(self):
        return self

    def next(self):
        if self.buffer:
            result = self._parse(self.eof)
            if result:
                return result
        while not self.eof:
            buffer = self.fo.read(self.read_size)
            self.eof = (len(buffer) != self.read_size)
            self.buffer += buffer
            result = self._parse(self.eof)
            if not result:
                continue
            raw, offset_b, offset_e = result
            return raw, offset_b, offset_e
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
        index = self.buffer.find(self.terminal)
        if index == -1:
            return None
        index += len(self.terminal)
        result = self.buffer[:index], self.pos, self.pos + index
        self.buffer = self.buffer[index:]
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
        match = self.preamble.search(self.buffer)
        if not match:
            logger.debug('%s[%s:%s] has no preamble', self.fo.name,
                self.pos, self.pos + len(self.buffer))
            return None
        if match.start() != 0:
            if self.strict:
                raise ValueError('%s[%s:%s] is partial block',
                    self.fo.name, self.pos, self.pos + match.start())
            logger.warning('%s[%s:%s] is partial block, discarding',
                self.fo.name, self.pos, self.pos + match.start())
            self.buffer = self.buffer[match.start():]
            self.pos += match.start()
        logger.debug('%s[%s:] has preamble', self.fo.name, self.pos)
        next = match
        while True:
            prev = next
            next = self.preamble.search(self.buffer, prev.end())
            if not next:
                logger.debug('%s[%s:] contains no preamble',
                    self.fo.name, self.pos + prev.end())
                break
            prefix = self.buffer[
                next.start() - len(self.terminal):next.start()]
            if prefix == self.terminal:
                logger.debug('%s[%s:] contains terminal-prefixed preamble',
                    self.fo.name, self.pos + next.end())
                break
            logger.debug('%s[%s:] contains non-terminal-prefixed preamble',
                self.fo.name, self.pos + next.end())
        if next:
            logger.debug('%s[%s:%s] hit', self.fo.name, self.pos,
                self.pos + next.start())
            raw = self.buffer[:next.start()]
            self.buffer = self.buffer[next.start():]
        else:
            if not eof:
                return None
            suffix = self.buffer[-len(self.terminal):]
            if suffix != self.terminal:
                return None
            logger.debug('%s[%s:] hit', self.fo.name, self.pos)
            raw = self.buffer
            self.buffer = ''
        result = raw, self.pos, self.pos + len(raw)
        self.pos += len(raw)
        return result
