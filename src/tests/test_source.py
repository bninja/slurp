import re

import slurp

from . import TestCase


class TestSource(TestCase):

    pattern = re.compile("""
(?P<ip>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\s+
-\s+
(?:(?P<user>\w+)|-)\s+
\[(?P<timestamp>\d{2}\/\w{3}\/\d{4}:\d{2}:\d{2}:\d{2})\]\s+
"(?:(?P<method>\w+)\s+(?P<uri>.+)\s+HTTP\/(?P<version>\d\.\d)|-)"\s+
(?:(?P<status>\d+)|-)\s+
(?:(?P<bytes>\d+)|-)\s+
"(?:(?P<referrer>.*?)|-)"\s+
"(?P<user_agent>.*?)"
(?:\s+(?:
request_time_seconds=(?:(?P<request_time_secs>\d+)|-)|
request_time_microseconds=(?:(?P<request_time_usecs>\d+)|-)|
guru_id=(?:(?P<guru_id>\w+)|-)|
\w+=.+?))*
""", re.VERBOSE)
    
    
    class Form(slurp.Form):
        
        ip = slurp.form.String()

        user = slurp.form.String(default=None)

        timestamp = slurp.form.Datetime(format='DD/MMM/YYYY:HH:mm:ss')

        method = slurp.form.String(default=None)

        uri = slurp.form.String(default=None)

        version = slurp.form.String(default=None)

        status = slurp.form.Integer(default=None)

        bytes = slurp.form.Integer(default=0)

    def test_no_form(self):
        src = slurp.Source(
            'test-source',
            globs=[],
            pattern=self.pattern,
            form=None,
        )
        for group, block in src.forms(self.open_fixture('sources', 'access.log')):
            self.assertIsInstance(group, dict)
            extras = set(group.keys()) - set([
                'status',
                'guru_id',
                'request_time_secs',
                'timestamp',
                'bytes',
                'uri',
                'request_time_usecs',
                'ip',
                'version',
                'user_agent',
                'referrer',
                'method',
                'user',
            ])
            self.assertEqual(extras, set([]))
            self.assertIsInstance(block, slurp.Block)
        

    def test_form(self):
        src = slurp.Source(
            'test-source',
            globs=[],
            pattern=self.pattern,
            form=self.Form,
        )
        for group, block in src.forms(self.open_fixture('sources', 'access.log')):
            self.assertIsInstance(group, self.Form)
            extras = set(group.keys()) - set([
                'status',
                'guru_id',
                'request_time',
                'timestamp',
                'bytes',
                'uri',
                'ip',
                'version',
                'user_agent',
                'referrer',
                'method',
                'user',
            ])
            self.assertEqual(extras, set([]))
            self.assertIsInstance(block, slurp.Block)

    def test_filter(self):
        src = slurp.Source(
            'test-source',
            globs=[],
            pattern=self.pattern,
            form=None,
            filter=lambda form, block: False,
        )
        self.assertEqual(
            list(src.forms(self.open_fixture('sources', 'access.log'))),
            []
        )
