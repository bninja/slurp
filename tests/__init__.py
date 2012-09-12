import os
import tempfile
import unittest2 as unittest

import mock

import slurp


BASE_PATH = os.path.abspath(os.path.dirname(__file__))
FIXTURE_PATH = os.path.join(BASE_PATH, 'fixtures')
TMP_PATH = tempfile.gettempdir()


class TestCase(unittest.TestCase):

    def _channel(self, **kwargs):
        return os.path.join(FIXTURE_PATH, *parts)

    def _fixture_file(self, *parts):
        return os.path.join(FIXTURE_PATH, *parts)

    def _tmp_file(self, *parts):
        return os.path.join(TMP_PATH, *parts)


class TestSeed(TestCase):

    pass


class TestTouch(TestCase):

    pass


class TestMonitor(TestCase):

    pass
