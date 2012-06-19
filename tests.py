import fnmatch
import json
import os
import re
import tempfile
import unittest

import mock

import slurp


BASE_PATH = os.path.abspath(os.path.dirname(__file__))
EXAMPLE_FILE_PATH = os.path.join(BASE_PATH, 'extras', 'example.py')
TMP_PATH = tempfile.gettempdir()


TestCase = unittest.TestCase

if not hasattr(TestCase, 'assertDictEqual'):
    def assertItemsEqual(self, a, b):
        self.assertEqual(sorted(a), sorted(b))

    TestCase.assertDictEqual = TestCase.assertEqual
    TestCase.assertItemsEqual = assertItemsEqual


class TestConf(TestCase):

    def test_create(self):
        conf = slurp.Conf(
            state_path=TMP_PATH,
            consumer_paths=[],
            locking=True,
            lock_timeout=30,
            tracking=True,
            event_sink=None,
            batch_size=256,
            )
        self.assertEqual(conf.state_path, TMP_PATH)
        self.assertEqual(conf.lock_class, slurp.FileLock)
        self.assertEqual(conf.lock_timeout, 30)
        self.assertEqual(conf.tracker_class, slurp.Tracker)
        self.assertEqual(conf.event_sink, None)
        self.assertEqual(conf.batch_size, 256)
        self.assertEqual(len(conf.consumers), 0)

    @mock.patch('slurp.Conf._load_consumers')
    def test_consumer_matching(self, _load_consumers):
        access_consumer = mock.Mock()
        error_consumer = mock.Mock()
        system_consumer = mock.Mock()
        _load_consumers.return_value = [
            ([re.compile(fnmatch.translate('*access.log*'))],
             access_consumer),
            ([re.compile(fnmatch.translate('*error.log*'))],
             error_consumer),
            ([re.compile(fnmatch.translate('*/cron')),
              re.compile(fnmatch.translate('*/messages'))],
             system_consumer),
            ]
        conf = slurp.Conf(
            state_path=TMP_PATH,
            consumer_paths=['/etc/slurp.d'],
            locking=True,
            lock_timeout=30,
            tracking=True,
            event_sink=None,
            batch_size=256,
            )
        consumers = conf.get_matching_consumers(
            '/my/logs/2012-05-28/access.log')
        self.assertItemsEqual(consumers, [access_consumer])
        consumers = conf.get_matching_consumers(
            '/my/logs/2012-05-28/error.log')
        self.assertItemsEqual(consumers, [error_consumer])
        consumers = conf.get_matching_consumers(
            '/my/logs/2012-05-28/cron')
        self.assertItemsEqual(consumers, [system_consumer])
        consumers = conf.get_matching_consumers(
            '/my/logs/2012-05-28/nada')
        self.assertItemsEqual(consumers, [])

    @mock.patch('slurp.Conf._load_consumers')
    def test_consumer_grouping(self, _load_consumers):
        access_consumer = mock.Mock(group='g1')
        error_consumer = mock.Mock(group='g1')
        system_consumer = mock.Mock(group='g2')
        _load_consumers.return_value = [
            ([re.compile(fnmatch.translate('*access.log*'))],
             access_consumer),
            ([re.compile(fnmatch.translate('*error.log*'))],
             error_consumer),
            ([re.compile(fnmatch.translate('*/cron')),
              re.compile(fnmatch.translate('*/messages'))],
             system_consumer),
            ]
        conf = slurp.Conf(
            state_path=TMP_PATH,
            consumer_paths=['/etc/slurp.d'],
            locking=True,
            lock_timeout=30,
            tracking=True,
            event_sink=None,
            batch_size=256,
            )
        self.assertItemsEqual(conf.get_consumer_groups(), ['g1', 'g2'])
        conf.filter_consumers(['g1', 'g2'])
        self.assertItemsEqual(conf.get_consumer_groups(), ['g1', 'g2'])
        conf.filter_consumers(['g1'])
        self.assertItemsEqual(conf.get_consumer_groups(), ['g1'])
        conf.filter_consumers(['g2'])
        self.assertItemsEqual(conf.get_consumer_groups(), [])


class TestConsumer(TestCase):

    def test_seed_backfill(self):
        tracker = mock.Mock()
        tracker.has.return_value = False
        c = slurp.Consumer(
                name='test',
                block_parser=mock.Mock(),
                event_parser=mock.Mock(),
                event_sink=mock.Mock(),
                tracker=tracker,
                lock=mock.Mock(),
                backfill=True)
        c.seed('/a/file/path')
        tracker.add.assert_called_once_with('/a/file/path', 0)
        tracker.save.assert_called_once_with()

    def test_seed_no_backfill(self):
        tracker = mock.Mock()
        tracker.has.return_value = False
        c = slurp.Consumer(
                name='test',
                block_parser=mock.Mock(),
                event_parser=mock.Mock(),
                event_sink=mock.Mock(),
                tracker=tracker,
                lock=mock.Mock(),
                backfill=False)
        with mock.patch('slurp.open', create=True) as mock_open:
            mock_open.return_value = mock.MagicMock(spec=file)
            fo = mock_open.return_value.__enter__.return_value
            fo.tell.return_value = 4321
            c.seed('/a/file/path')
            fo.seek.assert_called_once_with(0, os.SEEK_END)
            fo.tell.assert_called_once_with()
        tracker.add.assert_called_once_with('/a/file/path', 4321)
        tracker.save.assert_called_once_with()

    def test_eat(self):
        tracker = mock.Mock()
        tracker.has.return_value = True
        event_sink = mock.Mock()
        event_parser = mock.Mock()
        events = [
            mock.Mock(),
            mock.Mock(),
            mock.Mock(),
            ]
        event_parser.side_effect = events
        block_iterator = mock.Mock()
        block_iterator.return_value = [
            ('raw1', 12, 1212),
            ('raw2', 1213, 45454),
            ('raw3', 45455, 56565),
            ]
        c = slurp.Consumer(
                name='test',
                block_parser=block_iterator,
                event_parser=event_parser,
                event_sink=event_sink,
                tracker=tracker,
                lock=mock.Mock(),
                backfill=True)
        with mock.patch('slurp.open', create=True) as mock_open:
            mock_open.return_value = mock.MagicMock(spec=file)
            fo = mock_open.return_value.__enter__.return_value
            c.eat('/a/file/path')
        self.assertTrue(event_sink.called_count, 3)
        for i, event in enumerate(events):
            self.assertTrue(event_sink.call_args_list[i], event)
        tracker.get.assert_called_once_with('/a/file/path')
        tracker.save.assert_called_once_with()


class TestTracker(TestCase):

    file_offsets = {
        '/my/logs/2012-05-14/some-errors': 113571,
        '/my/logs/2012-05-20/some-errors': 3822300,
        '/my/otherlogs/2012-05-18/some-errors': 75964,
        '/my/otherlogs/2012-05-20/some-errors': 40701,
        '/my/logs/2012-05-30/some-errors': 874333,
        }

    @mock.patch('os.path.isfile')
    def test_load_nosuch(self, isfile):
        isfile.return_value = False
        tracker = slurp.Tracker('/a/non/existant/tracking/file')
        self.assertEqual(tracker.file_offsets, {})

    @mock.patch('os.path.isfile')
    def test_load(self, isfile):
        isfile.return_value = True
        with mock.patch('slurp.open', create=True) as mock_open:
            mock_open.return_value = mock.MagicMock(spec=file)
            fo = mock_open.return_value.__enter__.return_value
            fo.read.return_value = json.dumps(self.file_offsets)
            tracker = slurp.Tracker('/a/tracking/file')
        self.assertDictEqual(tracker.file_offsets, self.file_offsets)

    def test_save(self):
        tmp_fd, tmp_name = tempfile.mkstemp()
        os.close(tmp_fd)
        try:
            tracker = slurp.Tracker(tmp_name)
            for k, v in self.file_offsets.iteritems():
                tracker.add(k, v)
            tracker.save()
            tracker = slurp.Tracker(tmp_name)
            self.assertDictEqual(tracker.file_offsets, self.file_offsets)
        finally:
            if os.path.isfile(tmp_name):
                os.remove(tmp_name)

    @mock.patch('os.path.isfile')
    def test_remove_dir(self, isfile):
        isfile.return_value = False
        tracker = slurp.Tracker('/a/non/existant/tracking/file')
        for k, v in self.file_offsets.iteritems():
            tracker.add(k, v)
        tracker.remove_dir('/my/otherlogs')
        file_offsets = self.file_offsets.copy()
        del file_offsets['/my/otherlogs/2012-05-18/some-errors']
        del file_offsets['/my/otherlogs/2012-05-20/some-errors']
        self.assertDictEqual(tracker.file_offsets, file_offsets)


class TestBlockIterator(TestCase):

    class TmpFixture(object):

        def __init__(self, fixture):
            self.fixture = fixture
            self.fo = None
            self.tmp_path = None

        def __enter__(self):
            tmp_fd, self.tmp_path = tempfile.mkstemp()
            os.close(tmp_fd)
            with open(self.tmp_path, 'w') as fo:
                fo.write(self.fixture)
            self.fo = open(self.tmp_path, 'r')
            return self.fo

        def __exit__(self, exc_type, exc_value, traceback):
            if self.fo:
                self.fo.close()
                self.fo = None
            if os.path.isfile(self.tmp_path):
                os.remove(self.tmp_path)


class TestLineIterator(TestBlockIterator):

    fixture = """\
10.3.5.10 - - [06/Jun/2012:00:13:31] "GET /1 HTTP/1.0" 200 19 "-" "-"
10.3.4.10 - - [06/Jun/2012:00:13:32] "GET /2 HTTP/1.0" 200 19 "-" "-"
10.3.6.10 - - [06/Jun/2012:00:13:34] "GET /3 HTTP/1.0" 200 19 "-" "-"
10.3.5.10 - - [06/Jun/2012:00:13:36] "GET /4 HTTP/1.0" 200 19 "-" "-"
10.3.4.10 - - [06/Jun/2012:00:13:37] "GET /5 HTTP/1.0" 200 19 "-" "-"
10.3.6.10 - - [06/Jun/2012:00:13:39] "GET /6 HTTP/1.0" 200 19 "-" "-"
10.3.5.10 - - [06/Jun/2012:00:13:41] "GET /7 HTTP/1.0" 200 19 "-" "-"
10.3.4.10 - - [06/Jun/2012:00:13:42] "GET /8 HTTP/1.0" 200 19 "-" "-"
10.3.6.10 - - [06/Jun/2012:00:13:44] "GET /9 HTTP/1.0" 200 19 "-" "-"
"""

    terminal = '\n'

    def test_blocks(self):
        with self.TmpFixture(self.fixture) as fo:
            block_i = slurp.LineIterator(fo, self.terminal)
            blocks = list(block_i)
            self.assertEqual(
                blocks, [
("""\
10.3.5.10 - - [06/Jun/2012:00:13:31] "GET /1 HTTP/1.0" 200 19 "-" "-"
""", 0, 70),
("""\
10.3.4.10 - - [06/Jun/2012:00:13:32] "GET /2 HTTP/1.0" 200 19 "-" "-"
""", 70, 140),
("""\
10.3.6.10 - - [06/Jun/2012:00:13:34] "GET /3 HTTP/1.0" 200 19 "-" "-"
""", 140, 210),
("""\
10.3.5.10 - - [06/Jun/2012:00:13:36] "GET /4 HTTP/1.0" 200 19 "-" "-"
""", 210, 280),
("""\
10.3.4.10 - - [06/Jun/2012:00:13:37] "GET /5 HTTP/1.0" 200 19 "-" "-"
""", 280, 350),
("""\
10.3.6.10 - - [06/Jun/2012:00:13:39] "GET /6 HTTP/1.0" 200 19 "-" "-"
""", 350, 420),
("""\
10.3.5.10 - - [06/Jun/2012:00:13:41] "GET /7 HTTP/1.0" 200 19 "-" "-"
""",
 420, 490),
("""\
10.3.4.10 - - [06/Jun/2012:00:13:42] "GET /8 HTTP/1.0" 200 19 "-" "-"
""", 490, 560),
("""\
10.3.6.10 - - [06/Jun/2012:00:13:44] "GET /9 HTTP/1.0" 200 19 "-" "-"
""", 560, 630),
                ])


class TestMultiLineIterator(TestBlockIterator):

    fixture = """\
2012-06-06 00:00:09,912 : ERROR : some.channel : MainProcess : MainThread :
123
adadkljjk31he2k3h3jh23 e 23 23eh23 h23
123
12
312
2012-06-06 00:00:13,912 : ERROR : another.channel : MainProcess : MainThread :
123
123
123
12
312
2012-06-06 00:01:53,171 : ERROR : ya.channel : MainProcess : MainThread :
123das
123adasdasa  asd asd as da sd
12asdasdasdasd3
12
312
"""

    preamble = re.compile(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\,\d{3} : ')

    terminal = '\n'

    def test_blocks(self):
        with self.TmpFixture(self.fixture) as fo:
            block_i = slurp.MultiLineIterator(fo, self.preamble, self.terminal)
            blocks = list(block_i)
            self.assertEqual(blocks, [
("""\
2012-06-06 00:00:09,912 : ERROR : some.channel : MainProcess : MainThread :
123
adadkljjk31he2k3h3jh23 e 23 23eh23 h23
123
12
312
""", 0, 130),
("""\
2012-06-06 00:00:13,912 : ERROR : another.channel : MainProcess : MainThread :
123
123
123
12
312
""", 130, 228),
("""\
2012-06-06 00:01:53,171 : ERROR : ya.channel : MainProcess : MainThread :
123das
123adasdasa  asd asd as da sd
12asdasdasdasd3
12
312
""", 228, 362),
                ])
