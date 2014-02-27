"""
"""
import collections
import contextlib
import json
import logging
import os
import sqlite3
import subprocess
import tempfile
import time

from . import settings, Source, form


logger = logging.getLogger(__name__)


class ChannelSettings(settings.Form):

    #:
    strict = settings.Boolean(default=False)

    #:
    form = settings.Code(default=None)

    #:
    track = settings.Boolean(default=False)
    
    #: 
    backfill = settings.Boolean(default=False)

    #:
    enable = settings.Boolean(default=True)

    #:
    sources = settings.List(settings.String(), default=[])

    @sources.field.parse
    def sources(self, value):
        with self.ctx.reset():
            try:
                return value, self.ctx.config.source_settings(value)
            except Exception, ex:
                self.ctx.errors.invalid(self.ctx.field, str(ex))
                return settings.ERROR

    #:
    batch_size = settings.Integer(default=64)
    
    #:
    throttle_duration = settings.Integer(default=30)
    
    #:
    throttle_backoff = settings.Integer(default=2)
    
    #:
    throttle_cap = settings.Integer(default=1000)

    #: 
    sink = settings.String()

    @sink.parse
    def sink(self, value):
        with self.ctx.reset():
            try:
                return self.ctx.config.sink(value)
            except Exception, ex:
                self.ctx.errors.invalid(self.ctx.field, str(ex))
                return settings.ERROR



class Channel(object):

    def __init__(self,
            name,
            sink,
            enable=True,
            sources=None,
            form=None,
            state_dir=None,
            strict=ChannelSettings.strict.default,
            batch_size=ChannelSettings.batch_size.default,
            track=ChannelSettings.track.default,
            backfill=ChannelSettings.backfill.default,
            throttle_duration=ChannelSettings.throttle_duration.default,
            throttle_backoff=ChannelSettings.throttle_backoff.default,
            throttle_cap=ChannelSettings.throttle_cap.default,
        ):
        self.name = name
        self.enable = enable
        self.state_dir = state_dir
        self.sources = sources or []
        self.sink = sink
        self.batch_size = batch_size
        self.form = form
        if track:
            track_path = os.path.join(self.state_dir, self.name + '.track')
            self.tracker = Tracker(track_path)
        else:
            self.tracker = {}
        self.backfill = backfill
        self.throttle = Throttle(
            duration=throttle_duration,
            backoff=throttle_backoff,
            cap=throttle_cap,
        )
        self.strict = strict

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
        if not isinstance(self.tracker, dict):
            state['tracker'] = dict(self.tracker.items())
        else:
            logger.info('channel %s tracking is off', self.name)
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


class EditForm(form.Form):

    tracker = form.Dict(form.String(), form.Integer(), default=dict)


class Throttle(object):
    """
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
    """

    def __init__(self, path, timeout=None):
        self.path = path
        logger.debug('connecting to "%s"', self.path)
        self.cxn = sqlite3.connect(path, timeout=timeout or 0.0)
        with contextlib.closing(self.cxn.cursor()) as cur:
            cur.execute("""
                CREATE TABLE IF NOT EXISTS tracks (
                    path TEXT,
                    offset INTEGER,
                    PRIMARY KEY (path)
                )
                """
            )
            self.cxn.commit()
        
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
            raise ValueError('"{}" does not match pattern "{}"'.format(
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
                        '%s:%s %s @ %s - %s',
                        self.channel.name, self.name, fo.name, offset, errors[0]
                    )
                    continue
            yield form, offset
            
    def consume(self, path):
        if isinstance(path, basestring):
            return self.consume_path(path)
        else:
            return self.consume_stream(path)

    def consume_path(self, path):
        offset = None
        with self.open(path) as fo:
            while True:
                try:
                    for form, offset in self.forms(fo):
                        adjust = self.channel.sink(form, offset)
                        if offset:
                            offset = adjust
                            logger.info('%s:%s consume %s @ %s', self.channel.name, self.name, path, offset)
                            self.seek(path, offset.end)
                except KeyboardInterrupt:
                    raise
                except Exception, ex:
                    if self.channel.strict:
                        raise
                    logger.exception(ex)
                    continue
                finally:
                    adjust = self.channel.sink.flush()
                    if adjust:
                        offset = adjust
                        logger.info('%s:%s consume %s @ %s', self.channel.name, self.name, path, offset)
                        self.seek(path, offset.end)
                break
        if not offset:
            logger.info('%s:%s consume %s nothing', self.channel.name, self.name, path)
        return offset

    def consume_stream(self, fo):
        offset = None
        while True:
            try:
                for form, offset in self.forms(fo):
                    adjust = self.channel.sink(form, offset)
                    if adjust:
                        offset = adjust
                        logger.info('%s:%s consume %s @ %s', self.channel.name, self.name, fo.name, offset)
            except KeyboardInterrupt:
                raise
            except Exception, ex:
                if self.channel.strict:
                    raise
                logger.exception(ex)
                continue
            finally:
                adjust = self.channel.sink.flush()
                if adjust:
                    offset = adjust
                    logger.info('%s:%s consume %s @ %s', self.channel.name, self.name, fo.name, offset)
            break
        if not offset:
            logger.info('%s:%s consume %s nothing', self.channel.name, self.name, fo.name)
        return adjust
