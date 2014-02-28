"""
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

from . import settings, Source, form


logger = logging.getLogger(__name__)


class ChannelSettings(settings.Form):

    #: Flag indicating whether channel processing errors should be ignored or
    #: blocking.
    strict = settings.Boolean(default=False)

    #: Number of allowable consecutive errors before entering strict mode.
    strict_slack = settings.Integer(default=0).min(0)

    #: A module:attribute string that resolve to a callable with this signature:
    #:
    #: ..code::
    #:
    #:      def filter(form, block):
    #:          return True
    #:
    #: If the call-able returns True for block is processed otherwise it is
    #: discarded.
    filter = settings.Code(default=None).as_callable(lambda form, block: None)

    #: A module:attribute string that resolves to a :class:`Form`. This is used
    #: to map forms returned by a source.
    form = settings.Code(default=None)

    #: Flag indicating whether source progress information (i.e. offsets)
    #: should be persisted.
    track = settings.Boolean(default=False)

    #: Flag indicating whether newly tracked source files should be processed
    #: from the beginning of end upon detection.
    backfill = settings.Boolean(default=False)

    #: List of `Source` names.
    sources = settings.List(settings.String(), default=[])

    @sources.field.parse
    def sources(self, value):
        try:
            with self.ctx.reset():
                return value, self.ctx.config.source_settings(value)
        except settings.Error, ex:
            self.ctx.errors.append(ex)
            return settings.ERROR

    #:
    batch_size = settings.Integer(default=64)

    #: Initial number of seconds to throttle the channel on error.
    throttle_duration = settings.Integer(default=30)

    #: Back-off factor to apply to duration on a repeated error.
    throttle_backoff = settings.Integer(default=2)

    #: Maximum throttle duration.
    throttle_cap = settings.Integer(default=1000)

    #: Maximum number of events in channel processing queues. 0 means
    #: unbounded.
    queue_size = settings.Integer(default=1024).min(0)

    #: Channel processing queue poll frequency in seconds.
    queue_poll = settings.Float(default=10.0).min(1.0)

    #: `Sink` name.
    sink = settings.String()

    @sink.parse
    def sink(self, value):
        with self.ctx.reset():
            try:
                return self.ctx.config.sink(value)
            except Exception, ex:
                self.ctx.errors.invalid(str(ex))
                return settings.ERROR



class Channel(object):

    def __init__(self,
            name,
            sink,
            sources=None,
            filter=None,
            form=None,
            state_dir=None,
            strict=ChannelSettings.strict.default,
            strict_slack=ChannelSettings.strict_slack.default,
            batch_size=ChannelSettings.batch_size.default,
            track=ChannelSettings.track.default,
            backfill=ChannelSettings.backfill.default,
            throttle_duration=ChannelSettings.throttle_duration.default,
            throttle_backoff=ChannelSettings.throttle_backoff.default,
            throttle_cap=ChannelSettings.throttle_cap.default,
            queue_size=ChannelSettings.queue_size.default,
            queue_poll=ChannelSettings.queue_poll.default,
            stats=False,
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
        self.tracker = Tracker(track_path)
        self.sink = TrackingSink(sink, self.tracker)
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

    def match(self, path):
        for source in self.sources:
            if source.match(path):
                return source

    def add_source(self, *args, **kwargs):
        source = ChannelSource(self, *args, **kwargs)
        self.sources.append(source)
        return source

    @property
    def editor(self):
        editors = ['VISUAL', 'EDITOR']
        for name in editors:
            editor = os.getenv(name, None)
            if editor:
                break
        else:
            raise Exception('Must define one of {}'.format(', '.join(editors)))
        logger.debug('using editor "%s"', editor)
        return editor

    def edit(self):
        logger.debug('generating state')
        state = {}
        state['tracker'] = dict(self.tracker.items())
        raw = json.dumps(state, indent=4)

        raw_fd, raw_path = tempfile.mkstemp(prefix='slurp-')
        ctime = os.stat(raw_path).st_mtime
        with os.fdopen(raw_fd, 'w') as raw_fo:
            raw_fo.write(raw)

        cmd = [self.editor, raw_path]
        logger.debug('editing state file "%s"', ' '.join(cmd))
        subprocess.check_call(cmd)
        if os.stat(raw_path).st_mtime == ctime:
            os.remove(raw_path)
            return False

        with open(raw_path, 'r') as fo:
            form = EditForm(json.load(fo))
        logger.debug('applying state changes from "%s"', raw_path)
        for path, offset in form.tracker.iteritems():
            self.tracker[path] = offset
        for path in self.tracker.iterkeys():
            if path not in form.tracker:
                del self.tracker[path]
        os.remove(raw_path)

        return True

    def stats_sample(self):

        @contextlib.contextmanager
        def Dummy(*args, **kwargs):
            yield

        return (newrelic.agent.BackgroundTask if self.stats else Dummy)(
            self.stats_app,
            name=self.name,
        )

    def worker(self, **kwargs):
        return ChannelWorker(self, **kwargs)


class EditForm(form.Form):

    tracker = form.Dict(form.String(), form.Integer(), default=dict)


class TrackingSink(object):

    def __init__(self, sink, tracker):
        self.sink = sink
        self.tracker = tracker
        self.pending = {}

    def __enter__(self):
        return self

    def __exit__(self, type, value, traceback):
        self.flush()

    def _flushed(self):
        for path, offset in self.pending.iteritems():
            self.tracker[path] = offset
        self.pending.clear()

    def __call__(self, form, block):
        pending = self.sink(form, block)
        if not pending:
            self.tracker[block.path] = block.end
            self._flushed()
        else:
            self.pending[block.path] = block.end
        return pending

    def flush(self):
        self.sink.flush()
        self._flushed()


class Throttle(object):
    """
    Bounded back-off throttler used to temporarily disable channel source
    consumption when e.g. sink errors occur:

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
            raise ValueError('"{}" does not match pattern "{}"'.format(
                path, self.glob.pattern
            ))
        self.channel.tracker[path] = offset
        logger.debug('%s:%s "%s" offset %s', self.channel.name, self.name, path, offset)
        return offset

    def reset(self, path, offset=0):
        self.seek(path, offset)

    def tell(self, path):
        with self.open(path) as fo:
            return fo.tell()

    def touch(self, path):
        return self.seek(path, self.tell(path))

    def open(self, path):
        if not self.match(path):
            raise ValueError('"{0}" does not match pattern "{1}"'.format(
                path, self.glob.pattern
            ))
        logger.debug('%s:%s opening "%s"', self.channel.name, self.name, path)
        fo = open(path, 'r')
        if path in self.channel.tracker:
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
            if self.filter and not not self.filter(form, offset):
                logger.warning(
                    '%s:%s @ %s filtered', self.channel.name, self.name, offset
                )
                continue
            yield form, offset

    def consume(self, fo):
        if isinstance(fo, basestring):
            with self.open(fo) as fo:
                return self.consume(fo)
        path = getattr(fo, 'name', '<memory>')
        count = 0
        bytes = 0
        errors = 0
        st = time.time()
        with self.channel.stats_sample(), self.channel.sink:
            while True:
                try:
                    for form, block in self.forms(fo):
                        pending = self.channel.sink(form, block)
                        if not pending:
                            errors = 0
                        count += 1
                        bytes += block.end - block.begin
                except KeyboardInterrupt:
                    raise
                except Exception, ex:
                    if self.channel.strict:
                        raise
                    if (self.channel.strict_slack and
                        errors > self.channel.strict_slack):
                        logger.info(
                            '%s:%s consume consecutive errors %s > %s',
                            errors, self.channel.strict_slack,
                        )
                        raise
                    logger.exception(ex)
                    errors += 1
                    continue
                break
        et = time.time()
        offset = self.channel.tracker.get(path, None)
        logger.info(
            '%s:%s consumed %s (%s bytes) from "%s" @ %s in %0.4f sec(s)',
            self.channel.name, self.name, count, bytes, path, offset or '-', et - st
        )
        return count, offset


class ChannelWorker(threading.Thread):

    def __init__(self, channel, **kwargs):
        self.channel = channel
        self.throttle = Throttle(
            duration=channel.throttle_duration,
            cap=channel.throttle_cap,
            backoff=channel.throttle_backoff ,
        )
        if 'name' not in kwargs:
            kwargs['name'] = 'Channel-{0}'.format(channel.name)
        self.queue = Queue.Queue(maxsize=2)
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
               'channel "%s" worker queue is full @ %s, discarding ...',
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
            with source.open(event.path):
                count = source.consume(event.path)
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
        logger.info('entering channel "%s" event loop', self.channel.name)
        while True:
            if self.throttle:
                time.sleep(min(
                    max(0, time.time() - self.throttle.expires_at),
                    self.queue_poll
                ))
                continue
            try:
                event = self.queue.get(block=True, timeout=self.queue_poll)
            except Queue.Empty:
                continue
            if self.throttle:
                self.queue.task_done()
                self.enqueue(event)
                continue
            try:
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
                continue

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
