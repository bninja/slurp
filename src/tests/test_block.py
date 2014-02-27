import os

import slurp
from slurp.block import LineIterator, MultiLineIterator

from . import TestCase


class TestBlocks(TestCase):
    
    def test_offset(self):
        blocks = slurp.Blocks(self.open_fixture('sources', 'access.log'))
        self.assertEqual(blocks.tell(), 0)
        blocks.seek(0, os.SEEK_END)
        self.assertNotEqual(blocks.tell(), 0)
        blocks.seek(0)
        self.assertEqual(blocks.tell(), 0)

    def test_single_line(self):
        path = self.fixture('sources', 'access.log')
        blocks = slurp.Blocks(
            open(path, 'r'),
            strict=True,
            prefix=None,
            terminal='\n',
        )
        self.assertIsInstance(blocks.__iter__(), LineIterator)
        self.maxDiff = None
        self.assertListEqual([
                (path, 0, 309),
                (path, 309, 650),
                (path, 650, 987),
                (path, 987, 1312),
                (path, 1312, 1631),
                (path, 1631, 1950),
            ],
            map(lambda x: (x.path, x.begin, x.end), list(blocks))
        )

    def test_multi_line(self):
        path = self.fixture('sources', 'error.log')
        blocks = slurp.Blocks(
            open(path, 'r'),
            strict=True,
            prefix=r'\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2},\d{3} :',
            terminal='\n',
        )
        self.assertIsInstance(blocks.__iter__(), MultiLineIterator)
        self.maxDiff = None
        self.assertListEqual([
                (path, 0, 1326),
                (path, 1326, 1520),
                (path, 1520, 1714),
                (path, 1714, 4589),
                (path, 4589, 4783),
            ],
            map(lambda x: (x.path, x.begin, x.end), list(blocks))
        )
