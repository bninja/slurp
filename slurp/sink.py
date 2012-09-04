"""
Sinks used to process blocks (aka delimited strings) parsed by channels from
sources (aka files).
"""
import errno
import imp
import os
try:
    import ipdb as pdb
except ImportError:
    import pdb
import socket


class Sink(object):
    """
    Base class defining the sink interface. Note that sink doesn't need to be a
    class, just something that satisfies this interface (e.g. a function).
    """

    def __call__(self, blocks):
        """
        Something that processes blocks.

        :param blocks: A list of blocks. Each block is a tuple with:

                           1. Path to the file containing the block.
                           2. Offset in bytes to the beginning of the block.
                           3. Offset in bytes to the end of the block.
                           4. The block string itself (including its delimiters).

        :return: The number of blocks successfully processed. So e.g. if this
                 was 3 then blocks[3:] would be resubmitted to this sink by
                 the channel later.
        """
        raise NotImplementedError()


class DebugSink(Sink):
    """
    Debug breaks.
    """

    def __call__(self, blocks):
        count = len(blocks)
        pdb.set_trace()
        return count


class PythonSink(Sink):
    """
    Passes blocks along to a Python callable for processing.

    `spec`
        Specification for the Python callable with format:

            {module}:{eval}

        {module} can reference the containing module via a package path:

            my.module

        or file path:

            /my/moudle.py

        {eval} is soemthing that, when evaluated, results in a Python callable
        with the same signature as `Sink`:

        .. code:
            def my_callable(blocks):
                print 'i eat', len(blocks), 'blocks!'
                return len(blocks)
    """

    def __init__(self, spec):
        if isinstance(spec, basestring):
            module_path, expr = self._parse_spec(spec)
            self.func = self._import(module_path, expr)
        elif callable(spec):
            self.func = spec
        else:
            raise ValueError('Not string or callable')

    def __call__(self, blocks):
        return self.func(blocks)

    @staticmethod
    def _parse_spec(spec):
        parts = spec.split(':', 1)
        if len(parts) != 2:
            raise ValueError('Invalid python sink spec "{}"'.format(spec))
        return parts[0], parts[1]

    @staticmethod
    def _import(module_path, expr):
        if os.sep in module_path or module_path.endswith('.py'):
            module_name = os.path.basename(module_path)
            module_name = os.path.splitext(module_name)[0]
            try:
                module = imp.load_source(module_name, module_path)
            except IOError, ex:
                if ex.errno != errno.ENOENT:
                    raise
                raise ImportError('No such file {}'.format(module_path))
        else:
            module = __import__(module_path)
            module = reduce(getattr, module_path.split('.')[1:], module)
        globals = __builtins__
        if not isinstance(globals, dict):
            globals = globals.__dict__
        return eval(expr, globals, module.__dict__)


class FileSink(Sink):
    """
    Writes blocks to a file.

    `path`
        Path to file where blocks should be stored.
    """

    def __init__(self, path):
        self.fo = open(path, 'w')

    def __call__(self, blocks):
        # FIXME: not sure what format to use
        for path, raw, begin_off, end_off in blocks:
            self.fo.write(path)
            self.fo.write('\n')
            self.fo.write(str(begin_off))
            self.fo.write('\n')
            self.fo.write(str(end_off))
            self.fo.write('\n')
            self.fo.write(raw)
            self.fo.write('\n')
        self.fo.flush()
        return len(blocks)


class SocketSink(Sink):
    """
    Writes blocks to a socket.

    `address`
        Address of the server to which to write blocks. It can be either:

            - File path
            - IPv4 address
            - IPv6 address

    Intended to forward parsed blocks to another language or environment.
    """

    def __init__(self, address):
        family, self.address = self._parse_address(address)
        self.sock = socket.socket(self.family, socket.SOCK_STREAM)

    def __call__(self, blocks):
        try:
            return self._send(blocks)
        except:
            self.sock.close()
            self.sock.connect(self.address)
            return self._send(blocks)

    @staticmethod
    def _parse_address(address):
        # TODO
        raise NotImplemenetedError()


    def _send(self, block):
        # FIXME: not sure what protocol to use
        raise NotImplemenetedError()


class PipeSink(Sink):
    """
    Writes blocks to a pipe.

    `path`
        File path to use for the pipe.

    Intended to forward parsed blocks to another language or environment.
    """
    def __init__(self, path):
         raise NotImplemenetedError()

    def __call__(self, blocks):
        # FIXME: not sure what protocol to use
        raise NotImplemenetedError()


registry = {
    'dbg': DebugSink,
    'debug': DebugSink,
    'py': PythonSink,
    'python': PythonSink,
    'file': FileSink,
    'socket': SocketSink,
    'pipe': PipeSink,
    }
