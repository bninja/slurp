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


    def test_error_lax(self):
        src = slurp.Source(
            'test-source',
            globs=[],
            pattern=self.pattern,
            form=self.Form,
            strict=False,
        )
        io = self.io_fixture("""\
127.0.0.1 - - [20/Feb/2014:11:37:58] "POST /bank_accounts/BA3sCBsRa9KvqHqZnnV2n6UC/credits HTTP/1.1" 201 930 "-" "balanced-python/1.0.1beta2" request_time_seconds=0 request_time_microseconds=461208 build=- guru_id=OHM71e9d0d69a2311e38b8f069b72f5f856 merchant_guid=TEST-MR2gw65aJvFKph1RvviXqbSS marketplace_guid=TEST-MP2hfNOzdNcYKaoO78R6zpbw
gobble
127.0.0.1 - - [20/Feb/2014:11:37:58] "POST /credits/CR3sPVGSAkPYA9vp1lshEMv4/reversals HTTP/1.1" 201 740 "-" "balanced-python/1.0.1beta2" request_time_seconds=0 request_time_microseconds=622211 build=- guru_id=OHM72459e7a9a2311e38b8f069b72f5f856 merchant_guid=TEST-MR2gw65aJvFKph1RvviXqbSS marketplace_guid=TEST-MP2hfNOzdNcYKaoO78R6zpbw
a.b.c.d - - [20/Feb/2014:11:37:58] "POST /credits/CR3sPVGSAkPYA9vp1lshEMv4/reversals HTTP/1.1" 201 740 "-" "balanced-python/1.0.1beta2" request_time_seconds=0 request_time_microseconds=622211 build=- guru_id=OHM72459e7a9a2311e38b8f069b72f5f856 merchant_guid=TEST-MR2gw65aJvFKph1RvviXqbSS marketplace_guid=TEST-MP2hfNOzdNcYKaoO78R6zpbw
127.0.0.1 - - [20/Feb/2014:99:37:58] "POST /credits/CR3sPVGSAkPYA9vp1lshEMv4/reversals HTTP/1.1" 201 740 "-" "balanced-python/1.0.1beta2" request_time_seconds=0 request_time_microseconds=622211 build=- guru_id=OHM72459e7a9a2311e38b8f069b72f5f856 merchant_guid=TEST-MR2gw65aJvFKph1RvviXqbSS marketplace_guid=TEST-MP2hfNOzdNcYKaoO78R6zpbw
""")
        blocks = [
            (block.begin, block.end)
            for _, block in src.forms(io)
        ]
        self.assertListEqual([
            (0, 341), (348, 685)
        ], blocks)

    def test_error_strict(self):
        src = slurp.Source(
            'test-source',
            globs=[],
            pattern=self.pattern,
            form=self.Form,
            strict=True,
        )
        io = self.io_fixture("""\
127.0.0.1 - - [20/Feb/2014:11:37:58] "POST /bank_accounts/BA3sCBsRa9KvqHqZnnV2n6UC/credits HTTP/1.1" 201 930 "-" "balanced-python/1.0.1beta2" request_time_seconds=0 request_time_microseconds=461208 build=- guru_id=OHM71e9d0d69a2311e38b8f069b72f5f856 merchant_guid=TEST-MR2gw65aJvFKph1RvviXqbSS marketplace_guid=TEST-MP2hfNOzdNcYKaoO78R6zpbw
127.0.0.1 - - [20/Feb/2014:99:37:58] "POST /credits/CR3sPVGSAkPYA9vp1lshEMv4/reversals HTTP/1.1" 201 740 "-" "balanced-python/1.0.1beta2" request_time_seconds=0 request_time_microseconds=622211 build=- guru_id=OHM72459e7a9a2311e38b8f069b72f5f856 merchant_guid=TEST-MR2gw65aJvFKph1RvviXqbSS marketplace_guid=TEST-MP2hfNOzdNcYKaoO78R6zpbw
127.0.0.1 - - [20/Feb/2014:11:37:58] "POST /credits/CR3sPVGSAkPYA9vp1lshEMv4/reversals HTTP/1.1" 201 740 "-" "balanced-python/1.0.1beta2" request_time_seconds=0 request_time_microseconds=622211 build=- guru_id=OHM72459e7a9a2311e38b8f069b72f5f856 merchant_guid=TEST-MR2gw65aJvFKph1RvviXqbSS marketplace_guid=TEST-MP2hfNOzdNcYKaoO78R6zpbw
a.b.c.d - - [20/Feb/2014:11:37:58] "POST /credits/CR3sPVGSAkPYA9vp1lshEMv4/reversals HTTP/1.1" 201 740 "-" "balanced-python/1.0.1beta2" request_time_seconds=0 request_time_microseconds=622211 build=- guru_id=OHM72459e7a9a2311e38b8f069b72f5f856 merchant_guid=TEST-MR2gw65aJvFKph1RvviXqbSS marketplace_guid=TEST-MP2hfNOzdNcYKaoO78R6zpbw
""")
        blocks = []
        with self.assertRaises(ValueError):
            for _, block in src.forms(io):
                blocks.append((block.begin, block.end))
        self.assertListEqual([
            (0, 341)
        ], blocks)
