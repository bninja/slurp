import calendar
from datetime import datetime
import fnmatch
import json
import logging
import os
import re

from pyes import ES
import slurp


logger = logging.getLogger(__name__)


class AccessParser(slurp.EventParser):

    IP = r'(?P<ip>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}) -'
    USER = r'(?P<user>.+)'
    DATE = r'(?P<day>\d{2})\/(?P<month>\w{3})\/(?P<year>\d{4})'
    TIME = r'(?P<hour>\d{2}):(?P<min>\d{2}):(?P<sec>\d{2})'
    TIMESTAMP = r'\[' + DATE + ':' + TIME + r'\]'
    RESOURCE = r'\"((?P<method>\w+) (?P<uri>.+) HTTP\/(?P<version>\d\.\d)|-)\"'
    STATUS = r'(?P<status>\w+)'
    BYTES = r'(?P<bytes>\d+)'
    REFERRER = r'\"(?P<referrer>.*?)\"'
    USER_AGENT = r'\"(?P<user_agent>.+?)\"'

    PATTERN = ' '.join([
        IP, USER, TIMESTAMP, RESOURCE, STATUS, BYTES, REFERRER, USER_AGENT])
    RE = re.compile(PATTERN)

    BLOCK_TERMINAL = '\n'

    MONTHS = dict((calendar.month_abbr[i], i) for i in range(1, 12))

    def __call__(self, src_file, offset_b, offset_e, raw):
        host = src_file.rsplit(os.path.sep, 3)[-3]
        tag = os.path.splitext(os.path.basename(src_file))[0]
        match = self.RE.match(raw)
        if not match:
            raise ValueError('Unable to parse event from %s[%s:%s]' %
                (src_file, offset_b, offset_e))
        ip = match.group('ip')
        timestamp = datetime(
            year=int(match.group('year')),
            month=self.MONTHS[match.group('month')],
            day=int(match.group('day')),
            hour=int(match.group('hour')),
            minute=int(match.group('min')),
            second=int(match.group('sec')))
        user = match.group('user')
        if user == '-':
            user = None
        method = match.group('method')
        uri = match.group('uri')
        version = match.group('version')
        status = int(match.group('status'))
        severity = 'error' if status >= 500 else 'info'
        bytes = int(match.group('bytes'))
        referrer = match.group('referrer')
        if referrer == '-':
            referrer = None
        user_agent = match.group('user_agent')
        event = {
            'src_file': src_file,
            'offset_b': offset_b,
            'offset_e': offset_e,
            'tag': tag,
            'host': host,
            'severity': severity,
            'timestamp': timestamp,
            'payload': {
                'ip': ip,
                'user': user,
                'method': method,
                'uri': uri,
                'version': version,
                'status': status,
                'bytes': bytes,
                'referrer': referrer,
                'user_agent': user_agent,
                },
         }
        return event


class ErrorParser(slurp.EventParser):

    DATE = r'(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})'
    TIME = r'(?P<hour>\d{2}):(?P<min>\d{2}):(?P<sec>\d{2})\,(?P<usec>\d{3})'
    TIMESTAMP = DATE + ' ' + TIME
    SEVERITY = r'(?P<severity>\w+)'
    CHANNEL = r'(?P<channel>.+?)'
    PROCESS = r'.+?'
    THREAD = r'.+?'
    MESSAGE = r'(?P<message>.*)'

    PATTERN = ' : '.join([
        TIMESTAMP, SEVERITY, CHANNEL, PROCESS, THREAD, MESSAGE])
    RE = re.compile(PATTERN, flags=re.DOTALL)

    BLOCK_TERMINAL = '\n'
    BLOCK_PREAMBLE_PATTERN = ' : '.join([TIMESTAMP, SEVERITY, CHANNEL])
    BLOCK_PREAMBLE_RE = re.compile(BLOCK_PREAMBLE_PATTERN)

    def __call__(self, src_file, offset_b, offset_e, raw):
        host = src_file.rsplit(os.path.sep, 3)[-3]
        tag = os.path.splitext(os.path.basename(src_file))[0]
        match = self.RE.match(raw)
        if not match:
            raise ValueError('Unable to parse event from %s[%s:%s]' %
                (src_file, offset_b, offset_e))
        timestamp = datetime(
            year=int(match.group('year')),
            month=int(match.group('month')),
            day=int(match.group('day')),
            hour=int(match.group('hour')),
            minute=int(match.group('min')),
            second=int(match.group('sec')),
            microsecond=int(match.group('usec')))
        event = {
            'src_file': src_file,
            'offset_b': offset_b,
            'offset_e': offset_e,
            'tag': tag,
            'host': host,
            'severity': match.group('severity').lower(),
            'timestamp': timestamp,
            'payload': {
                'channel': match.group('channel'),
                'message': match.group('message'),
                },
         }
        return event


class SyslogParser(slurp.EventParser):

    FACILITY = r'(?P<facility>\d+)'
    SEVERITY = r'(?P<severity>\w+)'
    PREFIX = r'<' + FACILITY + r'\.' + SEVERITY + r'>'
    DATE = r'(?P<year>\d{4})-(?P<month>\d{2})-(?P<day>\d{2})'
    TIME = r'(?P<hour>\d{2}):(?P<min>\d{2}):(?P<sec>\d{2})(\.(?P<usec>\d{6})){0,1}'
    TZONE = r'(?P<tz_hour>\d{2}):(?P<tz_min>\d{2})'
    TIMESTAMP = DATE + r'T' + TIME + r'\+' + TZONE
    HOST = '(?P<host>.+?)'
    TAG = '(?P<tag>.+?)(\[(?P<pid>\d+)\]){0,1}:'
    MESSAGE = r'(?P<message>.*)'

    PATTERN = ' '.join([PREFIX, TIMESTAMP, HOST, TAG, MESSAGE])
    RE = re.compile(PATTERN)

    BLOCK_TERMINAL = '\n'
    BLOCK_PREAMBLE_PATTERN = ' '.join([PREFIX, TIMESTAMP])
    BLOCK_PREAMBLE_RE = re.compile(BLOCK_PREAMBLE_PATTERN)

    def __call__(self, src_file, offset_b, offset_e, raw):
        host = src_file.rsplit(os.path.sep, 3)[-3]
        tag = os.path.splitext(os.path.basename(src_file))[0]
        match = self.RE.match(raw)
        if not match:
            raise ValueError('Unable to parse event from %s[%s:%s]' %
                (src_file, offset_b, offset_e))
        usec = match.group('usec')
        usec = int(usec) if usec is not None else 0
        timestamp = datetime(
            year=int(match.group('year')),
            month=int(match.group('month')),
            day=int(match.group('day')),
            hour=int(match.group('hour')),
            minute=int(match.group('min')),
            second=int(match.group('sec')),
            microsecond=usec)
        event = {
            'src_file': src_file,
            'offset_b': offset_b,
            'offset_e': offset_e,
            'tag': tag,
            'host': host,
            'severity': match.group('severity').lower(),
            'timestamp': timestamp,
            'payload': {
                'facility_number': int(match.group('facility')),
                'message': match.group('message'),
                },
         }
        pid = match.group('pid')
        if pid is not None:
            event['payload']['pid'] = int(pid)
        return event


class SyslogJSONParser(SyslogParser):
    RE = re.compile(SyslogParser.PATTERN, flags=re.DOTALL)

    def __call__(self, src_file, offset_b, offset_e, raw):
        event = super(RequestParser, self).__call__(src_file, offset_b, offset_e, raw)
        event['payload']['message'] = json.loads(event['payload']['message'])
        return event


class ElasticSearchSink(object):
    def __init__(self, server, index, type):
        self.cxn = ES(server)
        self.index = index
        self.type = type

    def __call__(self, event):
        if isinstance(event, list):
            self.cxn.bulk_size = len(event)
            for e in event:
                self.cxn.index(e, self.index, self.type, bulk=True)
            self.cxn.flush_bulk()
        else:
            self.cxn.index(event, self.index, self.type)


ELASTIC_SEARCH_SERVER = '127.0.0.1:9200'


CONSUMERS = [
    # app-access
    {'name': 'app-access',
     'block_terminal': AccessParser.BLOCK_TERMINAL,
     'event_parser': AccessParser(),
     'event_sink': ElasticSearchSink(
         ELASTIC_SEARCH_SERVER, 'logs', 'http_access'),
     'batch_size': 256,
     'backfill': False,
     'patterns': [
         re.compile(fnmatch.translate('*/myapp-access*')),
         ],
     },

    # app-error
    {'name': 'app-error',
     'block_preamble': ErrorParser.BLOCK_PREAMBLE_RE,
     'block_terminal': ErrorParser.BLOCK_TERMINAL,
     'event_parser': ErrorParser(),
     'event_sink': ElasticSearchSink(
         ELASTIC_SEARCH_SERVER, 'logs', 'application'),
     'backfill': True,
     'patterns': [
         re.compile(fnmatch.translate('*/myapp-errors*')),
         ],
     },

    # app-request
    {'name': 'app-request',
     'block_preamble': SyslogJSONParser.BLOCK_PREAMBLE_RE,
     'block_terminal': SyslogJSONParser.BLOCK_TERMINAL,
     'event_parser': SyslogJSONParser(),
     'event_sink': ElasticSearchSink(
         ELASTIC_SEARCH_SERVER, 'logs', 'application_request'),
     'backfill': True,
     'patterns': [
         re.compile(fnmatch.translate('*/myapp-requests*')),
         ],
     },

    # sys
    {'name': 'sys',
     'block_preamble': SyslogParser.BLOCK_PREAMBLE_RE,
     'block_terminal': SyslogParser.BLOCK_TERMINAL,
     'event_parser': SyslogParser(),
     'event_sink': ElasticSearchSink(
         ELASTIC_SEARCH_SERVER, 'logs', 'system'),
     'batch_size': 4096,
     'backfill': False,
     'patterns': [
         re.compile(fnmatch.translate('*/boot.log')),
         re.compile(fnmatch.translate('*/cron')),
         re.compile(fnmatch.translate('*/haproxy')),
         re.compile(fnmatch.translate('*/mail')),
         re.compile(fnmatch.translate('*/messages')),
         re.compile(fnmatch.translate('*/secure')),
         re.compile(fnmatch.translate('*/postgres')),
         ],
     },
    ]
