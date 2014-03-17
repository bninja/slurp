import os
import shutil
from StringIO import StringIO
import threading
import time

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
        matches = [
            ('ts', '{0}/sources/nginx-access.log'.format(self.fixture())),
            ('ts', '{0}/sources/nginx-error.log'.format(self.fixture()))
        ]
        self.assertItemsEqual([('tc', matches, 9, 2589, 0)], results)
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
        matches = [
            ('ts', '{0}/sources/nginx-access.log'.format(self.fixture()))
        ]
        self.assertItemsEqual([
            ('tc', matches, 2, 536, 0),
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
        matches = [
            ('ts', '{0}/sources/nginx-access.log'.format(self.fixture())),
        ]
        self.assertItemsEqual([
            ('tc', matches, 0, 0, 6),
        ], results)
        self.assertDictEqual({
            '{0}/sources/nginx-access.log'.format(self.fixture()): 1449,
        }, dict(channel.tracker))



class TestWatch(TestCase):

    def _channel(self, **kwargs):
        settings = {
            'sink': slurp.Drop('tk'),
            'state_dir': self.tmp_dir(),
            'track': True,
            'backfill':True,
        }
        settings.update(kwargs)
        channel = slurp.Channel('tc', **settings)
        channel.add_source('ts', ['*/*', '*'], r'(?P<all>.*)')
        return channel


    def test_count(self):

        blocks = []
        watch_timeout = 20.0
        watch_delay = 1.0
        started_at = time.time()
        dir_path = self.tmp_dir()

        class Sink(slurp.Sink):

            def __call__(self, form, block):
                blocks.append((block.begin, block.end))

        channel = self._channel(sink=Sink('tk'), strict=False, backfill=True)

        def watch():
            slurp.watch([dir_path], [channel], timeout=watch_timeout, stop=stop)

        def stop(notifier):
            return len(blocks) >= 6 or started_at + watch_timeout < time.time()

        threads = [threading.Thread(target=watch)]
        for thread in threads:
            thread.daemon = True
            thread.start()

        time.sleep(watch_delay)

        file_path = os.path.join(dir_path, 'nginx-access.log')
        shutil.copyfile(self.fixture('sources', 'nginx-access.log'), file_path)

        for thread in threads:
            thread.join()

        self.assertListEqual([
            (0, 119),
            (119, 385),
            (385, 657),
            (657, 913),
            (913, 1177),
            (1177, 1449),
        ], blocks)
