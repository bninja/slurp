from slurp.channel import Tracker

from . import TestCase


class TestTracker(TestCase):

    def test_persist(self):
        path = self.tmp_file()
        tracker = Tracker(path)
        tracker.update({
            '/test/file/1': 1243,
            '/test/file/2': 321,
            '/test/file/3': 1232,
            '/test/file/4': 213124,
        })

    def test_iface(self):
        tracker = Tracker(':memory:')
        self.assertEqual(len(tracker), 0)
        self.assertEqual(tracker.items(), [])
        tracker['/test/file/1'] = 1243
        tracker['/test/file/2'] = 321
        tracker['/test/file/3'] = 1232
        tracker['/test/file/4'] = 213124
        self.assertEqual(len(tracker), 4)
        self.assertDictEqual({
                '/test/file/1': 1243,
                '/test/file/2': 321,
                '/test/file/3': 1232,
                '/test/file/4': 213124,
            },
            dict(tracker)
        )
        del tracker['/test/file/4']
        self.assertDictEqual({
                '/test/file/1': 1243,
                '/test/file/2': 321,
                '/test/file/3': 1232,
            },
            dict(tracker)
        )
        self.assertTrue('/test/file/1' in tracker)
        self.assertTrue('/test/file/2' in tracker)
        self.assertTrue('/test/file/3' in tracker)
        self.assertFalse('/test/file/4' in tracker)
