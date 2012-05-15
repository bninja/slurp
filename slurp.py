"""
Slurp parses "entries" from log files (a source) and passes them along to
something else (a sink). In everything here we assume that log files are
created, appended to and then possibly deleted. If not the file is not suitable
for use with slurp. This is the programming interface for slurp. If you need
command line access use the accompanying slurp script.

The specification for:
    - what files to consider
    - how to identify raw "entry" strings in them
    - how to parse those "entries" into something structured
    - where to send those parsed entries
is all encapsulated by something called a "consumer". Consumers are just
dictionaries defined in some python file. Here is an example:

    CONSUMERS = [
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

For more examples of consumers see contrib/examples.py.

Once you have these consumers defined you create a `Conf` object, passing
the path to the file with your consumer(s), and then either `seed`, `monitor`
or `eat` some log files:

    conf = Conf(
        state_path='/home/me/.slurp/state',
        consumer_paths=[
            '/home/me/.slurp/consumers/*.py',
            ],
        locking=True,
        lock_timeout=60,
        tracking=True)

    seed(['/var/log/messages'], conf)
    eat(['/var/log/messages]', conf)
    monitor(['/var/log/messages'], conf)

`seed` is used to initialize tracking information for a consumer which is just
the offset the the next un-consumed byte of a file considered by  a consumer.
The next time the consumer `eat`s the file it will resume from that offset.

`monitor` uses inotify (via pyinotify) to watch files considered by consumers
for changes. A change will trigger a `MonitorEvent` which will in turn call
`eat` for consumers considering the log file(s) affected by the monitor event.
"""
from __future__ import with_statement
import functools
import glob
import imp
import json
from lockfile import FileLock
import logging
import os
import pprint
import time

import pyinotify


__version__ = '0.0.1'

logger = logging.getLogger(__name__)


class Conf(object):
    """
    Has all configuration information necessary to run `seed`, `eat` and
    `monitor`.

    `state_path`
        Directory in which to store state information (i.e. tracking and lock
        files).

    `consumer_paths`
        A list of files containing consumer definitions. Each should be a
        python module files with a top level attribute CONSUMERS as a list
        of dictionaries. Each dictionary specifies the consumer and defines:
            - what files the consumer considers
            - how the consumer identifies raw "entry" strings
            - how the consumer parses those "entries" to something structured
            - where the consumer sends those parsed entries

    `locking`
        A flag indicating whether to perform locking or not. If locking is
        enabled then all consumer actions (i.e. `seed`, `eat`) as serialized.

    `lock_timeout`
        If locking is enabled this is the timeout in seconds to wait for a
        consumer to acquire its lock before failing.

    `tracking`
        A flag indicating whether to enable tracking what portion of a file
        has been processed by a consumer.

    `event_sink`
        Override where to send all events (i.e. parsed log entries). If None
        then the event_sink defined by the consumer is used. You really only
        need to supply this if you are debugging in which case you might want
        to use `null_sink` or `print_sink` and disable `tracking`.

    `batch_size`
        Override defining the number of events (i.e. parsed log entries) to
        send to an event sink at once. If None then the batch_size defined by
        the consumer is used.
    """
    def __init__(self,
            state_path, consumer_paths,
            locking, lock_timeout,
            tracking,
            event_sink,
            batch_size):
        self.state_path = state_path
        self.lock_class = FileLock if locking else DummyLock
        self.lock_timeout = lock_timeout
        self.tracker_class = Tracker if tracking else DummyTracker
        self.event_sink = event_sink
        self.batch_size = batch_size
        self.consumers = self._load_consumers(consumer_paths)

    def _load_consumers(self, paths):
        consumers = []
        consumer_names = set()
        for path in paths:
            file_paths = []
            if os.path.isfile(path):
                file_paths.append(path)
            else:
                if os.path.isdir(path):
                    path = os.path.join(path, '*.py')
                file_paths += [
                    p for p in glob.glob(path)]
            for file_path in file_paths:
                for conf in self._import_consumers(file_path):
                    patterns, consumer = self._create_consumer(**conf)
                    if consumer.name in consumer_names:
                        raise ValueError('consumer %s from %s conflict' %
                            (consumer.name, file_path))
                    consumers.append((patterns, consumer))
                    consumer_names.add(consumer.name)
        return consumers

    def _import_consumers(self, file_path):
        logger.debug('loading consumers from %s', file_path)
        dir_path = os.path.dirname(file_path)
        name = os.path.basename(file_path).rpartition('.')[0]
        module = imp.load_module(name, *imp.find_module(name, [dir_path]))
        if not hasattr(module, 'CONSUMERS'):
            logger.info('%s has not attribute CONSUMERS, skipping', file_path)
            return []
        return module.CONSUMERS

    def _create_consumer(self, **kwargs):
        logger.debug('creating consumer %s:\n%s',
            kwargs['name'], pprint.pformat(kwargs))
        patterns = kwargs.pop('patterns')
        block_terminal = kwargs.pop('block_terminal', '\n')
        block_preamble = kwargs.pop('block_preamble', None)
        if block_preamble:
            block_parser = functools.partial(MultiLineIterator,
                preamble=block_preamble,
                terminal=block_terminal)
        else:
            block_parser = functools.partial(LineIterator,
                terminal=block_terminal)
        kwargs['block_parser'] = block_parser
        file_path = os.path.join(self.state_path, kwargs['name'] + '.track')
        kwargs['tracker'] = self.tracker_class(file_path)
        file_path = os.path.join(self.state_path, kwargs['name'])
        kwargs['lock'] = self.lock_class(file_path)
        if self.event_sink:
            logger.debug('overriding consumer %s event sink to %s',
                kwargs['name'], self.event_sink.__name__)
            kwargs['event_sink'] = self.event_sink
        kwargs['lock_timeout'] = self.lock_timeout
        if self.batch_size is not None:
            logger.debug('overriding consumer %s batch size to %s',
                kwargs['name'], self.batch_size)
            kwargs['batch_size'] = self.batch_size
        return patterns, Consumer(**kwargs)

    def get_matching_consumers(self, file_path):
        consumers = []
        for patterns, consumer in self.consumers:
            for pattern in patterns:
                if pattern.match(file_path):
                    logger.debug('%s matched consumer %s pattern %s',
                        file_path, consumer.name, pattern.pattern)
                    consumers.append(consumer)
        return consumers


class Consumer(object):
    """
    Represents a consumer which defines:
        - what files to consider for consumption
        - how to identify raw "entry" strings in them
        - how to parse those "entries" to something structured
        - where to send those parsed entries

    These are typically specified as dictionaries in python files and passed to
    `Conf`. Which then create corresponding `Consumer` instances.

    `name`
        The name of the consumer.

    `block_parser`
        Any callable satisfying `BlockIterator`. It is used to iterate raw
        string entries in a log file. This is typically either `LineIterator`
        or `MultiLineIterator`.

    `event_parser`
        Any callable satisfying `EventParser`. It is used to take a raw string
        entry and convert it into something structured, typically a dict.

    `event_sink`
        Any callable satisfying `EventSink`. It is used to pass events parsed
        from the log to something else (e.g. a search server, database, etc).
        Only when this succeeds do we update the tracking offset for the
        log file from which the events were parsed.

    `tracker`
        Used to maintain the offsets past the portion of log files that have
        been consumed. It is typically either a `Tracker` or `DummyTracker`
        instance.

    `lock`
        A lock used to serialize all consumer actions. It is typically either a
        `LockFile` or `DummyLockFile` instance.

    `lock_timeout`
        Time in seconds to wait for a consumer to acquire its lock before
        timing out. It defaults to None which means no timeout.

    `backfill`
        Flag indicating whether `seed` should consume all pre-existing log
        file entries (i.e. tracking offset should be 0) or skipped (i.e.
        tracking offset should be set to the end of file). It defaults to True.

    `batch_size`
        Number of events (i.e. parsed log entries) to send to `event_sink` at
        once. It defaults to None which means no batching.
    """
    def __init__(self,
            name,
            block_parser, event_parser, event_sink,
            tracker,
            lock, lock_timeout=None,
            backfill=True,
            batch_size=None):
        self.name = name
        self.block_parser = block_parser
        self.event_parser = event_parser
        self.event_sink = event_sink
        self.tracker = tracker
        self.lock = lock
        self.lock_timeout = lock_timeout
        self.backfill = backfill
        self.batch_size = batch_size

    def seed(self, file_path):
        if self.tracker.has(file_path):
            logger.debug('%s already being tracked', file_path)
        else:
            if self.backfill:
                offset = 0
            else:
                with open(file_path, 'r') as fo:
                    fo.seek(0, os.SEEK_END)
                    offset = fo.tell()
            logger.debug('%s seeding %s with offset %s',
                self.name, file_path, offset)
            self.tracker.add(file_path, offset)
            self.tracker.save()

    def eat(self, file_path):
        logger.debug('locking %s with timeout %s',
            self.lock.lock_file, self.lock_timeout)
        self.lock.acquire(self.lock_timeout)
        logger.debug('locked %s with timeout %s',
            self.lock.lock_file, self.lock_timeout)
        try:
            if not self.tracker.has(file_path):
                if self.backfill:
                    offset = 0
                else:
                    with open(file_path, 'r') as fo:
                        fo.seek(0, os.SEEK_END)
                        offset = fo.tell()
                self.tracker.add(file_path, offset)
            offset = self.tracker.get(file_path)
            try:
                st = time.time()
                bytes = 0
                num_events = 0
                with open(file_path, 'r') as fo:
                    fo.seek(offset, os.SEEK_SET)
                    events = []
                    offset_e = offset
                    for raw, offset_b, offset_e in self.block_parser(fo):
                        bytes += len(raw)
                        num_events += 1
                        event = self.event_parser(
                            file_path, offset_b, offset_e, raw)
                        if event is None:
                            logger.warning(
                               'consumer %s parser returned nothing for '
                               '%s[%s:%s]',
                                self.name, file_path, offset_b, offset_e)
                            continue
                        events.append(event)
                        if not self.batch_size:
                            self.event_sink(events[0])
                            self.tracker.update(file_path, offset_e)
                            del events[:]
                        elif len(events) >= self.batch_size:
                            logger.debug('%s eating %s events',
                                self.name, len(events))
                            self.event_sink(events)
                            self.tracker.update(file_path, offset_e)
                            del events[:]
                    if events:
                        self.event_sink(events)
                        self.tracker.update(file_path, offset_e)
                        del events[:]
                et = time.time()
                logger.debug(
                    '%s ate %s events (%s bytes) from %s in %0.4f sec(s)',
                    self.name, num_events, bytes, file_path, et - st)
            finally:
                self.tracker.save()
        finally:
            logger.debug('unlocking %s', self.lock.lock_file)
            self.lock.release()

    def track(self, file_path):
        self.tracker.add(file_path)
        self.tracker.save()

    def untrack(self, file_path):
        self.tracker.remove(file_path)
        self.tracker.save()

    def untrack_dir(self, dir_path):
        self.tracker.remove_dir(dir_path)
        self.tracker.save()


class DummyTracker(object):
    """
    A dummy file offset tracker.

    `file_path`
        This it not used but present to be compatible with `Tracker` interface.
    """
    def __init__(self, file_path):
        self.file_path = file_path
        self.load()

    def load(self):
        pass

    def save(self):
        pass

    def add(self, file_path, offset=0):
        pass

    def update(self, file_path, offset):
        pass

    def get(self, file_path):
        return 0

    def has(self, file_path):
        return False

    def remove(self, file_path):
        pass

    def remove_dir(self, dir_path):
        pass


class Tracker(object):
    """
    A file offset tracker.

    `file_path`
        Where to persist offset tracking information.
    """
    def __init__(self, file_path):
        self.file_path = file_path
        self.load()

    def load(self):
        if os.path.isfile(self.file_path):
            logger.debug('loading tracking data from %s', self.file_path)
            with open(self.file_path, 'r') as fo:
                raw = fo.read()
                self.file_offsets = json.loads(raw)
        else:
            logger.debug('not tracking data %s', self.file_path)
            self.file_offsets = {}

    def save(self):
        logger.debug('saving tracking data to %s', self.file_path)
        with open(self.file_path, 'w') as fo:
            fo.write(json.dumps(self.file_offsets))

    def add(self, file_path, offset=0):
        logger.info('tracking add %s offset %s', file_path, offset)
        if file_path in self.file_offsets:
            raise ValueError('%s already tracked' % file_path)
        self.file_offsets[file_path] = offset

    def update(self, file_path, offset):
        logger.debug('tracking update %s to %s', file_path, offset)
        self.file_offsets[file_path] = offset

    def get(self, file_path):
        return self.file_offsets[file_path]

    def has(self, file_path):
        return file_path in self.file_offsets

    def remove(self, file_path):
        logger.info('tracking remove %s', file_path)
        del self.file_offsets[file_path]

    def remove_dir(self, dir_path):
        logger.info('tracking remove dir %s', dir_path)
        for file_path in self.file_offsets.keys():
            if file_path.startswith(dir_path):
                self.remove(file_path)


class DummyLock(object):
    """
    A dummy lock.

    `file_path`
        This it not used but present to be compatible with `FileLock`
        interface.
    """
    def __init__(self, file_path):
        self.lock_file = file_path

    def acquire(self, timeout=None):
        pass

    def release(self):
        pass


def get_files(path):
    """
    Converts a path to a list of files. If the path references a file we simply
    return a list containing only that file. Otherwise the path is a dictionary
    in which case all files in the dictionary are enumerated.

    :param path: A path to either a file or a directory.

    :return: A list paths to files.
    """
    if os.path.isfile(path):
        yield path
    else:
        for dir_name, dir_names, file_names in os.walk(path):
            for file_name in file_names:
                file_path = os.path.join(dir_name, file_name)
                yield file_path


class BlockIterator(object):
    """
    Base class for "block" parsers. A "block" within a file is just a delimited
    string. For log files these "blocks" are typically called entries. Derived
    classes need to determine how "blocks" are delimited.

    `fo`
        File-like object we are parsing for blocks.

    `strict`
        Flag indicating whether to fail or ignore malformed blocks. It
        defaults to False.

    `read_size`
        The number of bytes to read. It defaults to 2048.
    """
    def __init__(self, fo, strict=False, read_size=2048):
        self.fo = fo
        self.pos = fo.tell()
        self.strict = strict
        self.read_size = read_size
        self.buffer = ''
        self.eof = False

    def __iter__(self):
        return self

    def next(self):
        if self.buffer:
            result = self._parse(self.eof)
            if result:
                return result
        while not self.eof:
            buffer = self.fo.read(self.read_size)
            self.eof = (len(buffer) != self.read_size)
            self.buffer += buffer
            result = self._parse(self.eof)
            if not result:
                continue
            raw, offset_b, offset_e = result
            return raw, offset_b, offset_e
        if self.buffer:
            if self.strict:
                raise ValueError('%s[%s:] is partial block',
                    self.fo.name, self.pos)
            else:
                logger.warning('%s[%s:] is partial block, discarding',
                    self.fo.name, self.pos)
        raise StopIteration()

    def _parse(self, eof):
        raise NotImplemetedError()


class LineIterator(BlockIterator):
    """
    A block parser where all "blocks" are unambiguously delimited by a
    terminal string. Apache HTTP access logs are a good example of a line
    oriented block parser where the terminal is '\n'.

    `terminal`
        String used to delimit blocks.
    """
    def __init__(self, fo, terminal, **kwargs):
        super(LineIterator, self).__init__(fo, **kwargs)
        self.terminal = terminal

    def _parse(self, eof):
        index = self.buffer.find(self.terminal)
        if index == -1:
            return None
        index += len(self.terminal)
        result = self.buffer[:index], self.pos, self.pos + index
        self.buffer = self.buffer[index:]
        self.pos += index
        return result


class MultiLineIterator(BlockIterator):
    """
    A block parser where all "blocks" are delimited by a preamble (i.e. prefix)
    regex and a terminal string. Multi-line error logs are a good example of a
    multi-line oriented block parser.

    `preamble`
        Regex used to identify the beginning of a block.

    `terminal`
        String used to identify the end of a block.
    """
    def __init__(self, fo, preamble, terminal, **kwargs):
        super(MultiLineIterator, self).__init__(fo, **kwargs)
        self.preamble = preamble
        self.terminal = terminal

    def _parse(self, eof):
        match = self.preamble.search(self.buffer)
        if not match:
            logger.debug('%s[%s:%s] has no preamble', self.fo.name,
                self.pos, self.pos + len(self.buffer))
            return None
        if match.start() != 0:
            if self.strict:
                raise ValueError('%s[%s:%s] is partial block',
                    self.fo.name, self.pos, self.pos + match.start())
            logger.warning('%s[%s:%s] is partial block, discarding',
                self.fo.name, self.pos, self.pos + match.start())
            self.buffer = self.buffer[match.start():]
            self.pos += match.start()
        logger.debug('%s[%s:] has preamble', self.fo.name, self.pos)
        next = match
        while True:
            prev = next
            next = self.preamble.search(self.buffer, prev.end())
            if not next:
                logger.debug('%s[%s:] contains no preamble',
                    self.fo.name, self.pos + prev.end())
                break
            prefix = self.buffer[
                next.start() - len(self.terminal):next.start()]
            if prefix == self.terminal:
                logger.debug('%s[%s:] contains terminal-prefixed preamble',
                    self.fo.name, self.pos + next.end())
                break
            logger.debug('%s[%s:] contains non-terminal-prefixed preamble',
                self.fo.name, self.pos + next.end())
        if next:
            logger.debug('%s[%s:%s] hit', self.fo.name, self.pos,
                self.pos + next.start())
            raw = self.buffer[:next.start()]
            self.buffer = self.buffer[next.start():]
        else:
            if not eof:
                return None
            suffix = self.buffer[-len(self.terminal):]
            if suffix != self.terminal:
                if self.strict:
                    raise ValueError('%s[%s:%s] is partial block',
                        self.fo.name, self.pos, self.pos + len(self.buffer))
                logger.warning('%s[%s:%s] is partial block, discarding',
                    self.fo.name, self.pos, self.pos + len(self.buffer))
                self.pos += len(self.buffer)
                self.buffer = ''
                return None
            logger.debug('%s[%s:] hit', self.fo.name, self.pos)
            raw = self.buffer
            self.buffer = ''
        result = raw, self.pos, self.pos + len(raw)
        self.pos += len(raw)
        return result


class EventParser(object):
    """
    Interface for an event parser. Your event parsers don't need to derive from
    this class, they only need to be callable (so e.g. they can be simple
    functions).

    `src_file`
        Where the raw event was parsed from.

    `offset_b`
        Offset to the beginning of the raw event in `src_file`.

    `offset_e`
        Offset to the end of the raw event in `src_file`.

    `raw`
        The raw event string.
    """
    def __call__(self, src_file, offset_b, offset_e, raw):
        raise NotImplementedError()


class EventSink(object):
    """
    Interface for an event sink (i.e. where parsed events are passed along to).
    Your event sinks don't need to derive from this class, they only need to be
    callable (so e.g. they can be simple functions).

    `event`
        The parsed event. Your `EventParser` determines what this is (e.g.
        dict, integer, custom object, etc). Note that this can be a list of
        events if you are batch processing events. See `print_sink` for an
        example of that.
    """
    def __call__(self, event):
        raise NotImplementedError()
    
    
class EventFilter(EventSink):
    """
    Filtering event sink. Events for which _filter evaluates to True are
    passed along to `sink`, all others are dropped.

    `sink`
        An event sink to pass filtered (i.e. _filter(event) == True) events to.
    """

    def __init__(self, sink):
        self.sink = sink

    def _filter(self, event):
        raise NotImplementedError()
    
    def __call__(self, event):
        if isinstance(event, list):
            event = filter(self._filter, event)
        else:
            event = event if self._filter(event) else None
        if event:
            self.sink(event)


def print_sink(event):
    """
    An event sink which simply prints events. It can be useful when debugging
    in which case you pass it as the event_sink override to `Conf`.
    """
    if not isinstance(event, list):
        event = [event]
    for e in event:
        print e


def null_sink(event):
    """
    An event sink which does nothing. It can be useful when debugging in
    which case you pass it as the event_sink override to `Conf`.
    """
    pass


def seed(paths, conf):
    """
    Initialize tracking for paths for consumers loaded by conf.

    :param paths: Paths to track.
    :param conf: Instance of `Conf`.
    """
    for path in paths:
        logger.debug('scanning %s', path)
        path = path.strip()
        for file_path in get_files(path):
            consumers = conf.get_matching_consumers(file_path)
            for consumer in consumers:
                consumer.seed(file_path)


class MonitorEvent(pyinotify.ProcessEvent):
    """
    A pyinotify event fired when something being monitored changes (e.g. file
    in a monitored dictionary is created, monitored file is deleted, monitored
    file is modified, etc).

    :param conf: Instance of `Conf`.
    """
    def __init__(self, conf):
        self.conf = conf
        self.cached_matches = {}

    def process_default(self, event):
        logger.debug('processing event %s', event)

        # matching consumers
        if event.pathname not in self.cached_matches:
            consumers = self.conf.get_matching_consumers(event.pathname)
            self.cached_matches[event.pathname] = consumers
        consumers = self.cached_matches[event.pathname]

        # file created
        if (event.mask & pyinotify.IN_CREATE and
            not event.mask & pyinotify.IN_ISDIR):
            for consumer in consumers:
                consumer.track(event.pathname)

        # file modified
        if (event.mask & pyinotify.IN_MODIFY and
            not event.mask & pyinotify.IN_ISDIR):
            for consumer in consumers:
                consumer.eat(event.pathname)

        # file deleted
        if (event.mask & pyinotify.IN_DELETE and
            not event.mask & pyinotify.IN_ISDIR):
            for consumer in consumers:
                consumer.untrack(event.pathname)

        # directory deleted
        if (event.mask & pyinotify.IN_DELETE and
            event.mask & pyinotify.IN_ISDIR):
            for consumer in consumers:
                consumer.untrack_dir(event.pathname)


def monitor(paths, conf, callback=None):
    """
    Monitors directories and files for changes. Changes are communicated to
    consumers which react to the change accordingly (e.g. eat newly appended
    entries, etc).

    :param paths: Paths to dictionaries and files to monitor.
    :param conf: Instance of `Conf`.
    :param callback: Callable predicate used to terminate notification loop.
                     See pyinotify.Notifier for details.
    """
    mask = pyinotify.ALL_EVENTS
    wm = pyinotify.WatchManager()
    notifier = pyinotify.Notifier(wm, default_proc_fun=MonitorEvent(conf))
    for path in paths:
        path = path.strip()
        wm.add_watch(path, mask, rec=True, auto_add=True)
        logger.info('monitoring %s', path)
        seed([path], conf)  # TODO: allow disable?
        eat([path], conf)  # TODO: allow disable?
    logger.info('enter notification loop')
    notifier.loop(callback=callback)
    logger.info('exit notification loop')


def eat(paths, conf):
    """
    Feed files to consumers.

    :param paths: Paths to dictionaries and file to consume.
    :param conf: Instance of `Conf`.
    """
    for path in paths:
        path = path.strip()
        for file_path in get_files(path):
            num_consumed = 0
            consumers = conf.get_matching_consumers(file_path)
            for consumer in consumers:
                logger.debug('%s eating file %s', consumer.name, file_path)
                consumer.eat(file_path)
                num_consumed += 1
            if not num_consumed:
                logger.info('no consumers for file %s', file_path)
