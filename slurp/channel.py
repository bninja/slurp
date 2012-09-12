from functools import partial
import logging
import os
import Queue
import random
import threading
import time

import parse
import sink
import track


logger = logging.getLogger(__name__)


class Source(object):
    """
    Represents a class of files. Each file in the class is made up of uniformly
    delimited strings called blocks. The delimiting is determined by
    `preamble` and `terminal`.

    `name`
        Name given to this class of files (e.g. 'access-log').

    `patterns`
        Regexes used to determine whether a file should be classified as
        belonging to `Source`.

    `preamble`
        Regex used to determine the beginning of a block. If blocks are always
        single lines then a preamble is not needed should be None.

    `terminal`
        A string used to determine the end of a block (e.g. '\n').
    """

    def __init__(self, name, patterns, exclude_patterns, preamble, terminal):
        self.name = name
        self.patterns = patterns
        self.exclude_patterns = exclude_patterns
        if preamble:
            self.parser_cls = partial(
                parse.MultiLineIterator,
                preamble=preamble,
                terminal=terminal)
        else:
            self.parser_cls = partial(
                parse.LineIterator,
                terminal=terminal)

    def match(self, path):
        return (any(p.search(path) for p in self.patterns) and
                not any(p.search(path) for p in self.exclude_patterns))

    def parser(self, fo, **kwargs):
        return self.parser_cls(fo, **kwargs)


class Channel(object):
    """
    Maps `Source`s to a `Sink` as well as controlling the parsing, batching and
    throttling behavior used when pulling a block from a source and dispatching
    it to a sink.

    `name`
        The name of the channel.

    `srcs`
        Collection of `Source`s in which the channel is interested.

    `sink`
        A `Sink` to which blocks parsed from `Source`s should be sent.

    `tag`
        The channel tag. This is used during various operations to group or
        partition up channels (e.g. `Master` will group tagged channels with
        one `Worker`). If None then the channel is un-tagged. Defaults to None.

    `batch_size`
        The maximum number of blocks to pass to `sink`. Default to 1000. To
        disable batching set this to 1.

    `parse_strict`
        Flag indicating whether block parsing should fail (True) if malformed
        blocks are encountered or simply skipped (False). Defaults to True.

    `parse_read_size`
        The size in bytes of the buffer to read from a source files when
        parsing for blocks. Defaults to 2048.

    `backfill`
        Flag indicating whether to initialize a source file's tracking offset
        to 0 (True) or its end position (False). If True then blocks appended
        to a source file *before* being processed by the channel will still be
        parsed and sent along to its `sink`.

    `throttle_max`
        The maximum amount of time in seconds to throttle sending blocks from
        `srcs` to `sink`. Defaults to 600.

    `throttle_duration`
        The number of seconds to throttle the `sink`. Defaults to 10.

    `throttle_latency`
        The threshold in seconds above which to throttle a `sink`. If a `sink`
        takes longer than `throttle_latency` seconds to process blocks it will
        be throttled. If 0 or None then throttling is disabled. Defaults to 5.

    `throttle_deviation`
        The percentage of `threshold_duration` to randomly deviate throttle
        duration (e.g. 1.0 means a +/- 1 % randomized deviation from
        `throttle_duration`). Defaults to 2.

    `throttle_backoff`
        Flag indicating whether to apply exponential increase to
        `throttle_duration` if repeated throttles are encountered (True).
        Defaults to True.

    """

    def __init__(self,
            name,
            srcs,
            sink,
            batch_size=1000,
            backfill=True,
            tag=None,
            parse_strict=True,
            parse_read_size=2048,
            throttle_max=600.0,
            throttle_duration=5.0,
            throttle_latency=10.0,
            throttle_deviation=2.0,
            throttle_backoff=True):
        super(Channel, self).__init__()
        self.name = name
        self.srcs = srcs
        self.sink = sink
        self.tag = tag
        self.batch_size = batch_size
        self.parse_strict = parse_strict
        self.parse_read_size = parse_read_size
        self.backfill = backfill
        self.throttle_max = throttle_max
        self.throttle_latency = throttle_latency
        self.throttle_duration = throttle_duration
        self.throttle_deviation = throttle_deviation
        self.throttle_backoff = throttle_backoff

    def match(self, path):
        for src in self.srcs:
            if src.match(path):
                return src
        return None

    STOPPED     = 'stopped'
    THROTTLED   = 'throttled'
    EXHAUSTED   = 'exhausted'

    def __call__(self, event, tracker=None, stop=None):
        """
        Parsed blocks from the source associated with `event`, passing them
        along to `sink`. Parsing and sinking will continue until one of:

            - `event.src` has been exhausted
            - `stop` callback returns True
            - `sink` throttling is triggered by failure or high latency

        :param event: `Event` instance indicating which source was modified.
        :param tracker: Optional `Tracker` instance. Defaults to None.
        :param stop: Optional callback used to abort block parsing and sinking.

        :return: A tuple containing:

                     1. Exit disposition:

                            - `Channel.STOPPED`
                            - `Channel.THROTTLED`
                            - `Channel.EXHAUSTED`

                     2. Number of blocks parsed and successfully consumed by
                        `sink`
                     3. Number of bytes parsed and successfully consumed by
                        `sink`
        """
        if event.src not in self.srcs:
            raise ValueError(
                'Invalid source "{0}", expected one of {1}'.format(
                event.src.name,
                ','.join('"{}"'.format(s.name) for s in self.srcs)))
        num_blocks = 0
        num_bytes = 0
        tracker = tracker or track.DummyTracker()
        stop = stop or (lambda: False)
        try:
            with open(event.path, 'r') as fo:
                offset = tracker.get(self, event)
                if offset is None:
                    if self.backfill:
                        offset = 0
                    else:
                        fo.seek(0, os.SEEK_END)
                        offset = fo.tell()
                else:
                    fo.seek(offset, os.SEEK_SET)
                parser = event.src.parser(
                    fo,
                    strict=self.parse_strict,
                    read_size=self.parse_read_size)
                bytes = 0
                blocks = []
                for raw, offset_b, offset_e in parser:
                    if stop():
                        state = self.STOPPED
                        break
                    bytes += len(raw)
                    block = (event.path, offset_b, offset_e, raw)
                    blocks.append(block)
                    if self.batch_size and len(blocks) < self.batch_size:
                        continue
                    logger.debug('channel "%s" consuming %s block(s)',
                        self.name, len(blocks))
                    st = time.time()
                    count = self.sink(blocks)
                    delta = time.time() - st
                    if count:
                        tracker.set(self, event, blocks[count - 1][2])
                        num_blocks += count
                        num_bytes += sum(len(block[-1]) for blocks in blocks[:count])
                    logger.debug('channel "%s" consumed %s block(s) (%s byte(s)) in %0.4f sec(s)',
                        self.name, count, sum(len(block[-1]) for blocks in blocks[:count]), delta)
                    if count < len(blocks):
                        state = self.THROTTLED
                        break
                    if self.throttle_latency and delta > self.throttle_latency:
                        logger.info('channel "%s" sink latency %s exceeded throttle threshold %s',
                            self.name, delta, self.throttle_latency)
                        state = self.THROTTLED
                        break
                    del blocks[:]
                    bytes = 0
                else:
                    if blocks:
                        logger.debug('channel "%s" consuming %s block(s)',
                            self.name, len(blocks))
                        st = time.time()
                        count = self.sink(blocks)
                        delta = time.time() - st
                        if count:
                            tracker.set(self, event, blocks[count - 1][2])
                            num_blocks += count
                            num_bytes += sum(len(block[-1]) for blocks in blocks[:count])
                        logger.debug('channel "%s" consumed %s block(s) (%s byte(s)) in %0.4f sec(s)',
                            self.name, count, sum(len(block[-1]) for blocks in blocks[:count]), delta)
                        if count < len(blocks):
                            state = self.THROTTLED
                        elif self.throttle_latency and delta > self.throttle_latency:
                            logger.info('channel "%s" sink latency %s exceeded throttle threshold %s',
                                self.name, delta, self.throttle_latency)
                            state = self.THROTTLED
                        else:
                            state = self.EXHAUSTED
                        del blocks[:]
                        bytes = 0
                    else:
                        state = self.EXHAUSTED
        except IOError, io:
            if io.errono != errno.ENOENT:
                raise
            logger.debug('channel "%s" source %s has been deleted',
                self.name, event.path)
            tracker.delete(self, event)
            state = self.EXHAUSTED
        return state, num_blocks, num_bytes


def create_channels(channel_confs):
    """
    Creates `Channel`s based on passed configurations.

    :param channel_confs: Collection of dictionaries. Each dictionary
                          describes a channel and should have this form:

                          {
                              "name": ,
                              "sources": [
                                  {
                                  "name": string,
                                  "patterns": [regex, ...],
                                  "exclude_patterns": [regex, ...],
                                  "preamble": regex,
                                  "terminal": string,
                                  }
                                  ...
                                  ],
                              "sink": (string, string),
                              "batch_size": int,
                              "backfill": flag,
                              "tag": string,
                              "parse_strict": bool,
                              "parse_read_size": int,
                              "throttle_max": float,
                              "throttle_duration": float,
                              "throttle_latency": float,
                              "throttle_deviation": v,
                              "throttle_backoff": bool,
                          }

                          See `Source` and `Channels` for more details on these
                          fields.

    :return: List of `Channel` instances corresponding to `channel_confs`.
    """
    channels = []
    for channel_conf in channel_confs:
        channel_conf = channel_conf.copy()
        type, arg = channel_conf.pop('sink')
        if arg:
            channel_sink = sink.registry[type](arg)
        else:
            channel_sink = sink.registry[type]()
        channel_srcs = [Source(**source) for source in channel_conf.pop('sources')]
        channel = Channel(srcs=channel_srcs, sink=channel_sink, **channel_conf)
        channels.append(channel)
    return channels


class ChannelThread(threading.Thread):
    """
    Consumer thread for feeding `Source` `Event`s from a queue to a `Channel`.

    `channel`
        The channel associated with this thread.

    `tracking`
        Optional path to a tracking file to store for storing processing
        progress. Defaults to None.

    `forever`
        Optional flag indicating whether to consume `Event`s from the `event`
        queue until stopped (True) or exit after one `Event` has been consumed
        (False). Defaults to False.
    """

    poll_frequency = 1.0

    def __init__(self, channel, tracking=None, forever=False):
        super(ChannelThread, self).__init__()
        self.daemon = True
        self.channel = channel
        self.tracking = tracking
        self.forever = forever
        self.stop_event = threading.Event()
        self.throttle_event = threading.Event()
        self.throttle_count = 0
        self.backlog = {}
        self.events = Queue.Queue()

    def stopped(self):
        return self.stop_event.is_set()

    def stop(self):
        self.stop_event.set()

    def throttled(self):
        return self.throttle_event.is_set()

    def throttle(self, duration):
        logger.debug('throttling channel "%s" for %0.4f sec(s)',
            self.channel.name, duration)
        self.throttle_event.set()
        threading.Timer(duration, self.throttle_event.clear).start()

    def throttle_duration(self):
        duration = self.channel.throttle_duration
        if self.channel.throttle_backoff:
            duration *= self.throttle_count
        duration = min(duration, self.channel.throttle_max)
        if self.channel.throttle_deviation:
            deviation = duration * ((self.channel.throttle_deviation) / 2.0)
            duration += random.uniform(-1 * deviation, deviation)
        return duration

    def run(self):
        num_events = 0
        if self.tracking:
            tracker = track.Tracker(self.tracking)
        else:
            tracker = track.DummyTracker()
        try:
            while not self.stopped():
                try:
                    event = self.events.get(True, self.poll_frequency)
                except Queue.Empty:
                    if not self.throttled() and self.backlog:
                        logger.debug('draining %s event(s) from channel "%s" backlog',
                            len(self.backlog), self.channel.name)
                        map(self.events.put, self.backlog.itervalues())
                        self.backlog.clear()
                    elif not self.forever:
                        logger.debug('channel "%s" consumed all %s event(s)',
                            self.channel.name, num_events)
                        break
                    continue
                if self.throttled():
                    if event.path not in self.backlog:
                        self.backlog[event.path] = event
                    else:
                        self.backlog[event.path].merge(event)
                    continue
                st = time.time()
                state, num_blocks, num_bytes = self.channel(event, tracker, self.stopped)
                delta = time.time() - st
                self.events.task_done()
                num_events += 1
                logger.debug('channel "%s" consumed %s blocks (%s bytes) in %s sec(s) with disposition "%s"',
                    self.channel.name, num_blocks, num_bytes, delta, state)
                if state == Channel.THROTTLED:
                    self.throttle_count += 1
                    self.throttle(self.throttle_duration())
                    if event.path not in self.backlog:
                        self.backlog[event.path] = event
                    else:
                        self.backlog[event.path].merge(event)
                else:
                    self.throttle_count += 0
        except Exception, ex:
            logger.exception(ex)
            raise


class Event(object):
    """
    Represents an event, or events if merged with another, associated with a
    file. Merging happens when, for performance reasons, multiple events
    associated with a common `src` and `path` coalesce to one.

    `path`
        Path to the associated file.

    `flags`
        Bitwise OR of the operations:

            - `Event.CREATED_FLAG`
            - `Event.MODIFIED_FLAG`
            - `Event.DELETED_FLAG`

        that triggered the event. More than one can be set if this is a
        *merged* (aka coalesced) event.

    `src`
        `Source` associated with the `file`.
    """

    CREATED_FLAG    = 1 << 2
    MODIFIED_FLAG   = 1 << 3
    DELETED_FLAG    = 1 << 4

    def __init__(self, path, flags, src):
        self.path = path
        self.flags = flags
        self.src = src

    def merge(self, event):
        if event.path != self.path:
            raise ValueError('Unable to merge -- path %s != %s'.format(
                event.path, self.path))
        if event.src != self.src:
            raise ValueError('Unable to merge -- source %s != %s'.format(
                event.src, self.src))
        self.flags |= event.flags

    @property
    def created(self):
        return (self.type & self.CREATED_FLAG) != 0

    @property
    def modified(self):
        return (self.type & self.MODIFIED_FLAG) != 0

    @property
    def deleted(self):
        return (self.type & self.DELETED_FLAG) != 0
