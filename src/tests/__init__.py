import contextlib
import os
from StringIO import StringIO
import sys
import tempfile

try:
    import unittest
except ImportError:
    import unittest2 as unittest


class TestCase(unittest.TestCase):

    @classmethod
    def fixture(cls, *path):
        return os.path.join(
            os.path.abspath(os.path.dirname(__file__)),
            'fixtures',
            *path
        )

    @classmethod
    def open_fixture(cls, *path):
        return open(cls.fixture(*path), 'r')

    @classmethod
    def read_fixture(cls, *path):
        return cls.open_fixture(*path).read()

    @classmethod
    def tmp_dir(cls):
        return tempfile.mkdtemp()

    @classmethod
    def tmp_file(cls):
        return tempfile.mktemp(prefix='slurp-test-')

    # http://stackoverflow.com/a/5977043
    @classmethod
    @contextlib.contextmanager
    def capture_stream(cls, stream):
        prev = getattr(sys, stream)
        setattr(sys, stream, StringIO())
        try:
            yield getattr(sys, stream)
        finally:
            setattr(sys, stream, prev)

    @classmethod
    def capture_stderr(cls):
        return cls.capture_stream('stderr')

    @classmethod
    def capture_stdout(cls):
        return cls.capture_stream('stdout')
