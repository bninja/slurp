"""
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
    """

    def __call__(self, blocks):
        raise NotImplementedError()


class FileSink(Sink):
    """
    """

    def __init__(self, path):
        self.fo = open(path, 'w')

    def __call__(self, blocks):
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


class DebugSink(Sink):
    """
    """

    def __call__(self, blocks):
        count = len(blocks)
        pdb.set_trace()
        return count


class PythonSink(Sink):
    """
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


class SocketSink(Sink):
    """
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

    def _send(self, block):
        raise NotImplemenetedError()


registry = {
    'file': FileSink,
    'dbg': DebugSink,
    'debug': DebugSink,
    'py': PythonSink,
    'python': PythonSink,
    'socket': SocketSink,
    }
