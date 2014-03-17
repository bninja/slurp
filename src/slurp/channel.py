"""
A `Channel` is what:

    - consumes blocks from Source`s
    - passes them along to a `Sink`
    - tracks the what blocks have been processed by a `Sink`

"""
import collections
import contextlib
import errno
import json
import logging
import os
import Queue
import sqlite3
import subprocess
import tempfile
import time
import threading

try:
    import newrelic.agent
except ImportError:
    pass

from . import settings, Settings, Source, form, Form


logger = logging.getLogger(__name__)

NONE = object()


class ChannelSettings(Settings):

    #: A module:attribute string of a in-line code block that resolves to a
    #: callable with this  signature:
    #:
    #: ..code::
    #:
    #:      def filter(form, block):
    #:          return True
    #:
    #: If the callable returns True the block is processed otherwise it is
    #: discarded.
    filter = settings.Code(default=None).as_callable(lambda form, block: None)

    #: Flag indicating whether channel processing errors should be ignored or
    #: blocking.
    strict = settings.Boolean(default=False)

    #: Number of allowable consecutive errors before entering strict mode.
    strict_slack = settings.Integer(default=0).min(0)

    #: A module:attribute string or in-line code that resolves to a
    #: :class:`Form`. This is used to map forms returned by a source.
    form = settings.Code(default=None)

    #: Flag indicating whether source progress information (i.e. offsets)
    #: should be persisted.
    track = settings.Boolean(default=None)

    #: Flag indicating whether newly tracked source files should be processed
    #: from the beginning of end upon detection.
    backfill = settings.Boolean(default=None)

    @backfill.validate
    def backfill(self, value):
        if value is not None:
            if value and self.track is False:
                self.ctx.errors.invalid('Cannot backfill if "track" is false')
                return False
        return True

    #: List of `Source` names.
    sources = settings.List(settings.String(), default=[])

    @sources.field.parse
    def sources(self, path):
        section = path.primitive(basestring)
        if section not in self.ctx.config.source_names:
            self.ctx.errors.invalid('"{0}" is not a source'.format(section))
            return settings.ERROR
        try:
            with self.ctx.reset():
                return section, self.ctx.config.source_settings(section)
        except settings.Error, ex:
            self.ctx.errors.append(ex)
            return settings.ERROR

    #: Maximum number of block to process before forcing a sink flush.
    batch_size = settings.Integer(default=None)

    #: Initial number of seconds to throttle the channel on error.
    throttle_duration = settings.Integer(default=30)

    #: Back-off factor to apply to duration on a repeated error.
    throttle_backoff = settings.Integer(default=2)

    #: Maximum throttle duration.
    throttle_cap = settings.Integer(default=600)

    #: Maximum number of events in channel processing queues. 0 means
    #: unbounded.
    queue_size = settings.Integer(default=None).min(0)

    #: Channel processing queue poll frequency in seconds.
    queue_poll = settings.Float(default=None).min(1.0)

    #: Channel flush frequency in seconds. 0 means none.
    flush_frequency = settings.Float(default=None).min(0)

    #: `Sink` name.
    sink = settings.String()

    @sink.parse
    def sink(self, path):
        section = path.primitive(basestring)
        if section not in self.ctx.config.sink_names:
            self.ctx.errors.invalid('"{0}" is not a sink'.format(section))
            return settings.ERROR
        with self.ctx.reset():
            try:
                return self.ctx.config.sink(section)
            except Exception, ex:
                self.ctx.errors.invalid(str(ex))
                return settings.ERROR



class Channel(object):
    """
    A consumer used to feed blocks from a collections of `Source`s to a `Sink`.
    """

    def __init__(self,
            name,
            sink,
            sources=None,
            filter=None,
            form=None,
            state_dir=None,
            strict=False,
            strict_slack=0,
            batch_size=100,
            track=False,
            backfill=False,
            throttle_duration=30,
            throttle_backoff=2,
            throttle_cap=600,
            queue_size=1000,
            queue_poll=10.0,
            stats=False,
            flush_frequency=None,
        ):
        self.name = name
        self.state_dir = state_dir
        self.sources = sources or []
        self.batch_size = batch_size
        self.filter = filter
        self.form = form
        if track:
            track_path = os.path.join(self.state_dir, self.name + '.track')
        else:
            track_path = ':memory:'
        if self.state_dir:
            self.lock_file = os.path.join(self.state_dir, self.name + '.lock')
        else:
            self.lock_file = None
        self.tracker = Tracker(track_path)
        self.sink = sink
        self.backfill = backfill
        self.throttle_duration = throttle_duration
        self.throttle_backoff = throttle_backoff
        self.throttle_cap = throttle_cap
        self.queue_size = queue_size
        self.queue_poll = queue_poll
        self.strict = strict
        self.strict_slack = strict_slack
        self.stats = stats
        self.stats_app = newrelic.agent.application() if self.stats else None
        self.flush_frequency = flush_frequency

    def match(self, path):
        """
        Determines with a file path (or file object) matches any of this
        channels sources.
        """
        if isinstance(path, basestring):
            for source in self.sources:
                if source.match(path):
                    return source
            return
        if getattr(path, 'name', None):
            return self.match(self, path.name)
        if len(self.channel.sources) == 1:
            return self.channel.sources[0]
        return

    def add_source(self, *args, **kwargs):
        source = ChannelSource(self, *args, **kwargs)
        self.sources.append(source)
        return source

    def lock(self, timeout=None, poll=1.0):
        """
        Creates and advisory lock of this channel.
        """
        return ChannelLock(self, poll, timeout)

    @property
    def editor(self):
        editors = ['VISUAL', 'EDITOR']
        for name in editors:
            editor = os.getenv(name, None)
            if editor:
                break
        else:
            raise Exception('Must define one of {0}'.format(', '.join(editors)))
        logger.debug('using editor "%s"', editor)
        return editor

    def edit(self, io=None):
        """
        Edits the tracking state for this channel.

        :param io:
            File-like object to read new state from. If None we'll spawn an
            editor you can use to change state. Defaluts to None.

        :return:
            True if state was edited otherwise False.
        """
        if io is None:
            logger.debug('generating state')
            state = {}
            state['tracker'] = dict(self.tracker.items())
            raw = json.dumps(state, indent=4)

            raw_fd, raw_path = tempfile.mkstemp(prefix='slurp-')
            logger.debug('writing state to "%s"', raw_path)
            ctime = os.stat(raw_path).st_mtime
            with os.fdopen(raw_fd, 'w') as raw_fo:
                raw_fo.write(raw)

            cmd = [self.editor, raw_path]
            logger.debug('editing state - %s', ' '.join(cmd))
            subprocess.check_call(cmd)
            if os.stat(raw_path).st_mtime == ctime:
                logger.debug('no state changes detected')
                os.remove(raw_path)
                return False

            logger.debug('validating state changes from "%s"', raw_path)
            with open(raw_path, 'r') as fo:
                form = EditForm(json.load(fo))
        else:
            raw_path = '<memory>'
            logger.debug('validating state changes from "%s"', raw_path)
            form = EditForm(json.load(io))

        logger.info('applying state changes from "%s"', raw_path)
        for path, offset in form.tracker.iteritems():
            self.tracker[path] = offset
        for path in self.tracker.iterkeys():
            if path not in form.tracker:
                del self.tracker[path]
        if io is None:
            os.remove(raw_path)

        return True

    def worker(self, **kwargs):
        """
        Create a `ChannelWorker` used to asynchronously consume source blocks.
        """
        return ChannelWorker(self, **kwargs)

    def consumer(self):
        """
        Create a `ChannelConsumer` instances used to consume source blocks.
        """
        return ChannelConsumer(self)

    def consume(self, fo, source=None):
        """
        Convenience for consuming blocks from a source file.

        :param fo:
            Either a file-like object or a path to a file.

        :param source:
            The source for `fo`. If None the channel will match to a source.

        :return:
            A tuple of:

                - count of blocks consumed
                - bytes number of bytes in those blocks
                - errors number of blocks that could not be consumed due to an error
                - elapsed time

        """
        st = time.time()
        with self.consumer() as consume:
            consume(fo, source)
        et = time.time()
        return consume.count, consume.bytes, consume.errors, et - st


class ChannelConsumer(object):

    def __init__(self, channel):
        self.channel = channel

        self.sink = self.channel.sink
        self.reset_slack = self.channel.strict_slack
        self.slack = self.channel.strict_slack
        self.tracker = self.channel.tracker

        self.count = 0
        self.bytes = 0
        self.pending = 0
        self.errors = 0
        self.flush_at = None
        self.pending_tracker = {}

    def stats(self):

        @contextlib.contextmanager
        def Dummy(*args, **kwargs):
            yield

        return (newrelic.agent.BackgroundTask if self.channel.stats else Dummy)(
            self.channel.stats_app,
            name=self.channel.name,
        )

    @property
    def tally(self):
        return (self.count, self.pending, self.bytes, self.errors)

    def __call__(self, fo, source=None):
        # match and validate source
        if not source:
            source = self.channel.match(fo)
            if not source:
                raise ValueError('Unable to match "{0}" to {1}'.format(fo, self.channel))
        if source not in self.channel.sources:
            raise ValueError('"{0}" is not a {1} source'.format(source, self.channel))
        if isinstance(fo, basestring):
            path = fo
            offset = self.pending_tracker.get(fo, None)
            with source.open(path, offset=offset) as fo:
                return self.__call__(fo, source)

        path = getattr(fo, 'name', '<memory>')
        logger.debug('%s:%s consuming from "%s" ... ', self.channel.name, source.name, path)
        st = time.time()
        with self.stats():
            count, pending, bytes, errors = self.step(fo, source)
        et = time.time()
        offset = self.channel.tracker.get(path, None)
        delta = et - st

        logger.info(
            '%s:%s consumed %s (%s bytes) %s pending from "%s" @ %s in %0.4f sec(s)',
            self.channel.name, source.name, count, bytes, pending, path, offset or '-', delta
        )
        if not bytes:
            self.tracker[path] = fo.tell()
        return count, pending, bytes, errors

    def step(self, fo, source):
        count = 0
        pending = 0
        bytes = 0
        errors = 0
        block = None
        while True:
            try:
                for form, block in source.forms(fo):
                    # pending
                    if self.sink(form, block):
                        self.pending_tracker[block.path] = block.end
                        if not self.flush_at and self.channel.flush_frequency:
                            self.flush_at = time.time() + self.channel.flush_frequency
                        self.pending += 1
                        pending += 1
                        if self.pending >= self.channel.batch_size:
                            logger.info(
                                '%s:%s reached max batch size %s, flushing ...',
                                self.channel.name, source.name, self.channel.batch_size
                            )
                            self.flush()
                            count += pending
                            pending = 0
                    # emitted
                    else:
                        self.tracker[block.path] = block.end
                        self.flushed()
                        count += 1
                        pending = 0
                    self.bytes += block.end - block.begin
                    bytes += block.end - block.begin
            except Exception, ex:
                if not block:
                    raise
                self.error(ex, fo, block)
                errors += pending + 1
                pending = 0
                continue
            break
        return count, pending, bytes, errors

    @property
    def flush_poll(self):
        if not self.flush_at:
            return
        return max(0, time.time() - self.flush_at)

    @property
    def flush_expired(self):
        if not self.pending:
            return False
        if not self.flush_at:
            return True
        return time.time() > self.flush_at

    def flush(self):
        if self.pending:
            st = time.time()
            self.sink.flush()
            et = time.time()
            delta = et - st
            logger.info(
                '%s flushed %s in %0.4f sec(s)', self.channel.name, self.pending, delta
            )
            for path, offset in self.pending_tracker.iteritems():
                self.tracker[path] = offset
        self.flushed()

    def flushed(self):
        self.count += self.pending
        self.pending = 0
        self.flush_at = None
        self.slack = self.reset_slack
        self.pending_tracker.clear()

    def error(self, ex, fo, block):
        if self.channel.strict and self.slack <= 0:
            raise
        logger.exception(ex)
        self.slack -= 1
        self.errors += self.pending + 1
        self.pending = 0
        self.flush_at = None
        self.pending_tracker.clear()
        if block:
            self.channel.tracker[block.path] = block.end
            fo.seek(block.end)

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.flush()


class EditForm(Form):
    """
    Form for parsing `Channel` edit state.
    """

    tracker = form.Dict(form.String(), form.Integer(), default=dict)



class Throttle(object):
    """
    Bounded back-off throttler used to temporarily disable channel consumption
    when e.g. sink errors occur:

    .. code::

        if not source.channel.throttle:
            try:
                source.consume(path)
                source.channel.throttle.reset()
            except Exception:
                duration = source.channel.throttle()
                logger.exception(
                    'throttling channel %s for %s sec(s)', source.channel.name, duration,
                )

    """

    def __init__(self, duration, backoff=0, cap=None):
        self.duration = duration
        self.backoff = backoff
        self.cap = cap
        self.count = 0
        self.expires_at = None

    def reset(self):
        self.count = 0
        self.expires_at = None

    def __bool__(self):
        return self.__nonzero__()

    def __nonzero__(self):
        if not self.expires_at:
            return False
        if self.expires_at < time.time():
            self.expires_at = None
            return False
        return True

    def __call__(self):
        duration = self.duration + self.duration * self.backoff * self.count
        if self.cap and duration > self.cap:
            duration = self.cap
        self.expires_at = time.time() + duration
        self.count += 1
        return duration


class CannotLock(RuntimeError):

    def __init__(self, channel, timeout, poll, attempts):
        super(CannotLock, self).__init__(
            'Channel {0} lock "{1}"cannot be acquired - timeout={2}, poll={3}, attempts={4}'
            .format(channel.name, channel.lock_file, timeout, poll, attempts)
        )


class ChannelLock(object):

    def __init__(self, channel, timeout=None, poll=1.0):
        self.channel = channel
        self.fo = open(channel.lock_file, 'w')
        self.timeout = timeout
        self.poll = poll

    def acquire(self, timeout=NONE, poll=NONE):
        import fcntl

        timeout = self.timeout if timeout is NONE else timeout
        poll = self.poll if timeout is NONE else poll

        logger.debug(
            'acquiring channel %s lock ("%s") ...',
            self.channel.name, self.channel.lock_file
        )
        flags = fcntl.LOCK_EX
        if not timeout:
            flags |= fcntl.LOCK_NB
        else:
            timeout_at = time.time() + timeout
        attempts = 0
        while True:
            try:
                attempts += 1
                fcntl.lockf(self.fo.fileno(), flags)
            except IOError as ex:
                if ex.errno not in (errno.EACCES, errno.EAGAIN):
                    raise
                if not self.timeout or timeout_at < time.time():
                    raise CannotLock(self.channel, timeout, poll)
            logger.info(
                'channel %s locked ("%s") on attempt %s, sleeping %s sec(s)',
                self.channel.name, self.channel.lock_file, poll, attempts,
            )
            time.sleep(poll)
        logger.debug(
            'channel %s locked ("%s") on attempt %s',
            self.channel.name, self.channel.lock_file, attempts,
        )
        return self

    def release(self):
        import fcntl

        fcntl.lockf(self.fo.fileno(), fcntl.LOCK_UN)
        logger.debug(
            'channel %s unlocked ("%s")',
            self.channel.name, self.channel.lock_file
        )

    def __enter__(self):
        return self.acquire()

    def __exit__(self, type, value, traceback):
        self.release()


class Tracker(collections.MutableMapping):
    """
    File offset tracking as a mutable map backed by as sqlite db.
    """

    def __init__(self, path, timeout=None):
        self.path = path
        self.timeout = timeout
        self._cxn = None

    @property
    def cxn(self):
        if self._cxn:
            return self._cxn
        logger.debug('connecting to "%s"', self.path)
        cxn = sqlite3.connect(self.path, timeout=self.timeout or 0.0)
        with contextlib.closing(cxn.cursor()) as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS tracks (
                    path TEXT,
                    offset INTEGER,
                    PRIMARY KEY (path)
                )
                """
            )
            cxn.commit()
        self._cxn = cxn
        return self._cxn

    def __getitem__(self, key):
        with contextlib.closing(self.cxn.cursor()) as cur:
            cur.execute("""
                SELECT offset
                FROM tracks
                WHERE path = ?
                """,
                (key,)
            )
            track = cur.fetchone()
        if not track:
            raise KeyError(key)
        return track[0]

    def __setitem__(self, key, value):
        with contextlib.closing(self.cxn.cursor()) as cur:
            cur.execute("""
                UPDATE tracks
                SET offset = ?
                WHERE path = ?
                """,
                (value, key))
            if cur.rowcount == 0:
                cur.execute("""
                    INSERT INTO tracks
                    (path, offset)
                    VALUES
                    (?, ?)
                    """,
                    (key, value)
                )
            self.cxn.commit()
        logger.debug('track ("%s", "%s") offset %s', self.path, key, value)

    def __delitem__(self, key):
        with contextlib.closing(self.cxn.cursor()) as cur:
            cur.execute("""
                DELETE FROM tracks
                WHERE path = ?
                """,
                (key,)
            )
            if cur.rowcount == 0:
                raise KeyError()
            self.cxn.commit()
        logger.debug('track ("%s", "%s") deleted', self.path, key)

    def __iter__(self):
        with contextlib.closing(self.cxn.cursor()) as cur:
            cur.execute('SELECT path FROM tracks')
            rows = cur.fetchall()
        for row in rows:
            yield row[0]

    def __len__(self):
        with contextlib.closing(self.cxn.cursor()) as cur:
            cur.execute('SELECT COUNT(*) FROM tracks')
            return cur.fetchone()[0]


class ChannelSource(Source):

    def __init__(self, channel, *args, **kwargs):
        self.channel = channel
        super(ChannelSource, self).__init__(*args, **kwargs)

    def seek(self, path, offset):
        if not self.match(path):
            raise ValueError('"{0}" does not match pattern "{1}"'.format(
                path, self.glob.pattern
            ))
        self.channel.tracker[path] = offset
        logger.debug('%s:%s "%s" offset %s', self.channel.name, self.name, path, offset)
        return offset

    def reset(self, path, offset=0):
        if not self.match(path):
            raise ValueError('"{0}" does not match pattern "{1}"'.format(
                path, self.glob.pattern
            ))
        if path in self.channel.tracker:
            del self.channel.tracker[path]
            logger.debug('%s:%s "%s" offset reset', self.channel.name, self.name, path)

    def tell(self, path):
        with self.open(path) as fo:
            return fo.tell()

    def touch(self, path):
        return self.seek(path, self.tell(path))

    def open(self, path, offset=None):
        if not self.match(path):
            raise ValueError('"{0}" does not match pattern "{1}"'.format(
                path, self.glob.pattern
            ))
        logger.debug('%s:%s opening "%s"', self.channel.name, self.name, path)
        fo = open(path, 'r')
        if offset:
            fo.seek(offset, os.SEEK_SET)
        elif path in self.channel.tracker:
            offset = self.channel.tracker[path]
            fo.seek(offset, os.SEEK_SET)
        elif not self.channel.backfill:
            fo.seek(0, os.SEEK_END)
        logger.debug('%s:%s "%s" @ %s', self.channel.name, self.name, path, fo.tell())
        return fo

    def forms(self, fo):
        for form, offset in super(ChannelSource, self).forms(fo):
            if self.channel.form:
                src = form
                form = self.channel.form()
                errors = form(src)
                if errors:
                    if self.strict:
                        raise ValueError()
                    logger.warning(
                        '%s:%s @ %s failed - %s',
                        self.channel.name, self.name, offset, errors[0]
                    )
                    continue
            if self.channel.filter and not self.channel.filter(form, offset):
                continue
            yield form, offset

    def consume(self, fo):
        return self.channel.consume(fo, self)


class ChannelWorker(threading.Thread):

    def __init__(self, channel, **kwargs):
        self.channel = channel
        self.consume = self.channel.consumer()
        self.throttle = Throttle(
            duration=channel.throttle_duration,
            cap=channel.throttle_cap,
            backoff=channel.throttle_backoff ,
        )
        if 'name' not in kwargs:
            kwargs['name'] = 'Channel-{0}'.format(channel.name)
        self.queue = Queue.Queue(maxsize=self.channel.queue_size)
        self.queue_poll = self.channel.queue_poll
        self.matches = {}
        super(ChannelWorker, self).__init__(**kwargs)

    def match(self, path):
        if path not in self.matches:
            self.matches[path] = self.channel.match(path)
        return self.matches[path]

    # queue

    def enqueue(self, event):
        try:
            self.queue.put(event, block=False)
            return True
        except Queue.Full:
            logger.debug(
               'channel %s worker queue is full @ %s, discarding ...',
               self.channel.name, self.queue.qsize(),
            )
            return False

    # event handlers

    def on_create_file(self, event):
        return self.on_modify_file(event)

    def on_modify_file(self, event):
        source = self.match(event.path)
        if not source:
            return 0
        try:
            count, bytes, pending, errors = self.consume(event.path, source)
            if count:
                self.throttle.reset()
        except IOError, ex:
            if ex.errno == errno.ENOENT:
                self.on_delete_file()
            raise
        return count

    def on_delete_file(self, event):
        self.matches.pop(event.path)

    # event loop

    def run(self):
        logger.info('entering channel %s event loop', self.channel.name)
        while True:
            try:
                if self.throttle:
                    time.sleep(max(0, time.time() - self.throttle.expires_at))
                self.step()
            except Exception:
                duration = self.throttle()
                logger.exception(
                    'throttling channel %s worker for %s sec(s)',
                    self.channel.name, duration
                )

    def step(self):
        if self.consume.flush_expired:
            self.consume.flush()
        try:
            timeout = self.queue_poll
            if self.consume.flush_poll:
                timeout = min(self.consume.flush_poll, self.queue_poll)
            event = self.queue.get(block=True, timeout=timeout)
        except Queue.Empty:
            return False
        if self.throttle:
            self.queue.task_done()
            self.enqueue(event)
            return False
        try:
            if self.consume.flush_expired:
                self.consume.flush()
            if event.is_delete:
                self.on_delete_file(event)
            elif event.is_create:
                self.on_create_file(event)
            elif event.is_modify:
                self.on_modify_file(event)
            self.queue.task_done()
        except Exception:
            duration = self.throttle()
            logger.exception(
                'throttling channel %s worker for %s sec(s)',
                self.channel.name, duration
            )
            self.enqueue(event)
        return True


class ChannelEvent(collections.namedtuple('ChannelEvent', ['path', 'flags'])):

    # flags

    CREATE = 1 << 0
    MODIFY = 1 << 1
    DELETE = 1 << 2

    @property
    def is_create(self):
        return (self.CREATE & self.flags) != 0

    @property
    def is_modify(self):
        return (self.MODIFY & self.flags) != 0

    @property
    def is_delete(self):
        return (self.DELETE & self.flags) != 0

    # creates

    @classmethod
    def create(cls, path):
        return cls(path=path, flags=cls.CREATE)

    @classmethod
    def modify(cls, path):
        return cls(path=path, flags=cls.MODIFY)

    @classmethod
    def delete(cls, path):
        return cls(path=path, flags=cls.DELETE)
