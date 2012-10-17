import os
import re
import tempfile

from slurp.parse import MultiLineIterator, LineIterator

from . import TestCase


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

"""

    terminal = '\n'

    def test_blocks(self):
        with open(self._fixture_file('single_line_1'), 'r') as fo:
            block_i = LineIterator(fo, self.terminal)
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

    preamble = re.compile(r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\,\d{3} : ')

    terminal = '\n'

    def test_blocks(self):
        with open(self._fixture_file('multi_line_1'), 'r') as fo:
            block_i = MultiLineIterator(fo, self.preamble, self.terminal)
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
