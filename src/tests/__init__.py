import contextlib
import os
from StringIO import StringIO
import sys
import tempfile

try:
    import unittest2 as unittest
except ImportError:
    import unittest


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
    def io_fixture(cls, raw):
        return StringIO(raw)

    @classmethod
    def tmp_dir(cls):
        return tempfile.mkdtemp()

    @classmethod
    def tmp_file(cls, dir=None):
        return tempfile.mktemp(prefix='slurp-test-', dir=dir)
