import os

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
