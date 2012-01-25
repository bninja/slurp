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
        return imp.load_source('', file_path).CONSUMERS

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
                    for raw, offet_b, offset_e in self.block_parser(fo):
                        bytes += len(raw)
                        num_events += 1
                        event = self.event_parser(
                            file_path, offet_b, offset_e, raw)
                        if event is None:
                            logger.warning(
                                'consumer %s parser returned nothing for:\n%s',
                                self.name, raw)
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
    def __init__(self, file_path):
        self.lock_file = file_path

    def acquire(self, timeout=None):
        pass

    def release(self):
        pass


def get_files(path):
    if os.path.isfile(path):
        yield path
    else:
        for dir_name, dir_names, file_names in os.walk(path):
            for file_name in file_names:
                file_path = os.path.join(dir_name, file_name)
                yield file_path


class BlockIterator(object):
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
            raw, offet_b, offset_e = result
            return raw, offet_b, offset_e
        if self.buffer:
            if self.strict:
                raise ValueError('%s[%s:] is partial block',
                    self.fo.name, self.pos)
            else:
                logger.warning('%s[%s:] is partial block, discarding',
                    self.fo.name, self.pos)
        raise StopIteration()


class LineIterator(BlockIterator):
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
    def __call__(self, src_file, offset_b, offset_e, raw):
        raise NotImplementedError()


class EventSink(object):
    def __call__(self, event):
        raise NotImplementedError()


def print_sink(event):
    if not isinstance(event, list):
        event = [event]
    for e in event:
        print e


def null_sink(event):
    pass


def seed(paths, conf):
    for path in paths:
        logger.debug('scanning %s', path)
        path = path.strip()
        for file_path in get_files(path):
            consumers = conf.get_matching_consumers(file_path)
            for consumer in consumers:
                consumer.seed(file_path)


class MonitorEvent(pyinotify.ProcessEvent):
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


def monitor(paths, conf):
    mask = pyinotify.ALL_EVENTS
    wm = pyinotify.WatchManager()
    for path in paths:
        path = path.strip()
        wm.add_watch(path, mask, rec=True, auto_add=True)
        logger.debug('monitoring %s', path)
        seed([path], conf)  # TODO: allow disable?
        eat([path], conf)  # TODO: allow disable?
    notifier = pyinotify.Notifier(wm, default_proc_fun=MonitorEvent(conf))
    logger.debug('enter notification loop')
    notifier.loop()
    logger.debug('exit notification loop')


def eat(paths, conf):
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
