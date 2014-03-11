from StringIO import StringIO

import slurp

from . import TestCase


class TestTouch(TestCase):

    def test_file_backfill(self):
        channel = slurp.Channel(
            'tc',
            slurp.Drop,
            state_dir=self.tmp_dir(),
            track=True,
            backfill=True
        )
        channel.add_source('ts', ['*/er*'], r'(?P<all>.*)')
        results = list(slurp.touch(
            [self.fixture('sources', 'nginx-access.log'),
             self.fixture('sources', 'error.log'),
             self.fixture('sources', 'nginx-error.log'),
             ],
            [channel],
        ))
        self.assertItemsEqual([
            ('tc', 'ts', '{0}/sources/error.log'.format(self.fixture()), 0),
        ], results)
        self.assertDictEqual({
            '{0}/sources/error.log'.format(self.fixture()): 0,
        }, dict(channel.tracker))

    def test_file_no_backfill(self):
        channel = slurp.Channel(
            'tc',
            slurp.Drop,
            state_dir=self.tmp_dir(),
            track=True,
            backfill=False,
        )
        channel.add_source('ts', ['*/nginx-*'], r'(?P<all>.*)')
        results = list(slurp.touch(
            [self.fixture('sources', 'nginx-access.log'),
             self.fixture('sources', 'error.log'),
             self.fixture('sources', 'nginx-error.log'),
             ],
            [channel],
        ))
        self.assertItemsEqual([
            ('tc', 'ts', '{0}/sources/nginx-access.log'.format(self.fixture()), 1449),
            ('tc', 'ts', '{0}/sources/nginx-error.log'.format(self.fixture()), 1140),
        ], results)
        self.assertDictEqual({
            '{0}/sources/nginx-access.log'.format(self.fixture()): 1449,
            '{0}/sources/nginx-error.log'.format(self.fixture()): 1140,
        }, dict(channel.tracker))

    def test_file_none(self):
        channel = slurp.Channel('tc', slurp.Drop)
        channel.add_source('ts', ['*/nope'], r'(?P<all>.*)')
        results = list(slurp.touch(
            [self.fixture('sources', 'nginx-access.log'),
             self.fixture('sources', 'error.log'),
             self.fixture('sources', 'nginx-error.log'),
             ],
            [channel],
        ))
        self.assertItemsEqual([], results)
        self.assertDictEqual({}, dict(channel.tracker))


class TestTell(TestCase):

    def test(self):
        channel = slurp.Channel(
            'tc',
            slurp.Drop,
            state_dir=self.tmp_dir(),
            track=True,
            backfill=False,
        )
        self.assertDictEqual({}, dict(channel.tracker))
        channel.add_source('ts', ['*/nginx-*'], r'(?P<all>.*)')
        results = list(slurp.tell(
            [self.fixture('sources', 'nginx-access.log'),
             self.fixture('sources', 'error.log'),
             self.fixture('sources', 'nginx-error.log'),
             ],
            [channel],
        ))
        self.assertItemsEqual([
            ('tc', 'ts', '{0}/sources/nginx-access.log'.format(self.fixture()), 1449),
            ('tc', 'ts', '{0}/sources/nginx-error.log'.format(self.fixture()), 1140),
        ], results)
        self.assertDictEqual({}, dict(channel.tracker))


class TestReset(TestCase):

    def test(self):
        channel = slurp.Channel(
            'tc',
            slurp.Drop,
            state_dir=self.tmp_dir(),
            track=True,
            backfill=False,
        )
        channel.add_source('ts', ['*/nginx-*'], r'(?P<all>.*)')

        # touch
        result = list(slurp.touch(
            [self.fixture('sources', 'nginx-access.log'),
             self.fixture('sources', 'error.log'),
             self.fixture('sources', 'nginx-error.log'),
             ],
            [channel],
        ))
        self.assertItemsEqual([
            ('tc', 'ts', '{0}/sources/nginx-access.log'.format(self.fixture()), 1449),
            ('tc', 'ts', '{0}/sources/nginx-error.log'.format(self.fixture()), 1140),
            ], result)
        self.assertDictEqual({
            '{0}/sources/nginx-access.log'.format(self.fixture()): 1449,
            '{0}/sources/nginx-error.log'.format(self.fixture()): 1140,
            }, dict(channel.tracker))

        # reset
        results = list(slurp.reset(
            [self.fixture('sources', 'nginx-access.log'),
             self.fixture('sources', 'error.log'),
             self.fixture('sources', 'nginx-error.log'),
             ],
            [channel],
        ))
        self.assertItemsEqual([
            ('tc', 'ts', '{0}/sources/nginx-access.log'.format(self.fixture())),
            ('tc', 'ts', '{0}/sources/nginx-error.log'.format(self.fixture())),
        ], results)
        self.assertDictEqual({}, dict(channel.tracker))


class TestConsume(TestCase):

    def _channel(self, **kwargs):
        settings = {
            'sink': slurp.Drop('tk'),
            'state_dir': self.tmp_dir(),
            'track': True,
            'backfill':True,
        }
        settings.update(kwargs)
        channel = slurp.Channel('tc', **settings)
        channel.add_source('ts', ['*/nginx-*'], r'(?P<all>.*)')
        return channel

    def test_count(self):
        channel = self._channel()
        self.assertDictEqual({}, dict(channel.tracker))
        results = list(slurp.consume(
            [self.fixture('sources', 'nginx-access.log'),
             self.fixture('sources', 'error.log'),
             self.fixture('sources', 'nginx-error.log'),
             ],
            [channel],
        ))
        self.assertItemsEqual([
            ('tc', 'ts', '{0}/sources/nginx-access.log'.format(self.fixture()), 6, 1449, 0),
            ('tc', 'ts', '{0}/sources/nginx-error.log'.format(self.fixture()), 3, 1140, 0),
            ], results)
        self.assertDictEqual({
            '{0}/sources/nginx-access.log'.format(self.fixture()): 1449,
            '{0}/sources/nginx-error.log'.format(self.fixture()): 1140,
        }, dict(channel.tracker))

    def test_stream(self):
        channel = self._channel()
        self.assertDictEqual({}, dict(channel.tracker))
        with self.open_fixture('sources', 'nginx-access.log') as fo:
            list(slurp.consume([fo], [channel]))
        self.assertDictEqual({
            '{0}/sources/nginx-access.log'.format(self.fixture()): 1449,
        }, dict(channel.tracker))

    def test_memory(self):
        channel = self._channel()
        self.assertDictEqual({}, dict(channel.tracker))
        fo = StringIO(self.read_fixture('sources', 'nginx-access.log'))
        list(slurp.consume([fo], [channel]))
        self.assertDictEqual({
            '<memory>'.format(self.fixture()): 1449,
        }, dict(channel.tracker))

    def test_pattern_errors(self):
        channel = self._channel(strict=False)
        del channel.sources[:]
        channel.add_source('ts', ['*/nginx-*'], r'98\.210\.157\.178\s+-\s+z')
        results = list(
            slurp.consume([self.fixture('sources', 'nginx-access.log')], [channel])
        )
        self.assertItemsEqual([
            ('tc', 'ts', '{0}/sources/nginx-access.log'.format(self.fixture()), 2, 536, 0),
        ], results)
        self.assertDictEqual({
            '{0}/sources/nginx-access.log'.format(self.fixture()): 1449,
        }, dict(channel.tracker))

    def test_sink_errors(self):

        class _Sink(slurp.Sink):

            def __call__(self, form, block):
                poop

        channel = self._channel(sink=_Sink('tk'), strict=False)
        results = list(
            slurp.consume([self.fixture('sources', 'nginx-access.log')], [channel])
        )
        self.assertItemsEqual([
            ('tc', 'ts', '{0}/sources/nginx-access.log'.format(self.fixture()), 0, 0, 6),
        ], results)
        self.assertDictEqual({
            '{0}/sources/nginx-access.log'.format(self.fixture()): 1449,
        }, dict(channel.tracker))



class TestWatch(TestCase):


    pass
