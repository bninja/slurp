"""
"""
from collections import defaultdict
import logging
import os

import pyinotify

from channel import Event, Source, Channel, ChannelThread, create_channels
from conf import load as load_conf
from master import Master
from track import Tracker, DummyTracker


__version__ = '0.3.0'


logger = logging.getLogger(__name__)


def _files(path):
    path
    if os.path.isfile(path):
        yield path
    else:
        for dir_name, dir_names, file_names in os.walk(path):
            for file_name in file_names:
                file_path = os.path.join(dir_name, file_name)
                if os.path.isfile(file_path):
                    yield file_path


def seed(channels, paths, tracking=None):
    """
    """
    if tracking:
        tracker = Tracker(tracking)
    else:
        tracker = DummyTracker()
    for path in paths:
        for file_path in _files(path):
            with open(file_path, 'r') as fo:
                fo.seek(0, os.SEEK_END)
                offset = fo.tell()
            for channel in channels:
                if channel.backfill:
                    continue
                src = channel.match(path)
                if not src:
                    continue
                logger.info('seeding "%s" for channel "%s" @ %s',
                    file_path, channel.name, offset)
                event = Event(file_path, Event.MODIFIED_FLAG, src)
                tracker.set(channel, event, offset)


def touch(channels, paths, tracking=None, callback=None):
    """
    """
    channel_events = defaultdict(list)
    for path in paths:
        for file_path in _files(path):
            for channel in channels:
                src = channel.match(path)
                if not src:
                    continue
                logger.info('touching "%s" for channel "%s"',
                    file_path, channel.name)
                event = Event(file_path, Event.MODIFIED_FLAG, src)
                channel_events[channel].append(event)

    channel_thds = []
    for channel, events in channel_events.iteritems():
        channel_thd = ChannelThread(channel, tracking)
        map(channel_thd.events.put, events)
        channel_thd.start()
        channel_thds.append(channel_thd)

    while channel_thds:
        channel_thds[0].join(1.0)
        if not channel_thds[0].is_alive():
            channel_thds.pop(0)


class _MonitorEvent(pyinotify.ProcessEvent):

    def __init__(self, channel_thds):
        self.channel_thds = channel_thds
        self.matches = {}

    def process_default(self, event):
        logger.debug('processing event %s', event)

        # deleted
        if event.mask & pyinotify.IN_ISDIR:
            if event.mask & pyinotify.IN_DELETE:
                ctx.tracker.delete_prefix(event.pathname)
            for k in self.matches.keys():
                if k.startswith(event.pathname):
                    del self.matches[k]

        # matching channel sources
        if event.pathname not in self.matches:
            matches = []
            for channel_thd in self.channel_thds:
                src = channel_thd.channel.match(event.pathname)
                if not src:
                    break
                matches.append((channel_thd, src))
            self.matches[event.pathname] = matches
        matches = self.matches[event.pathname]
        if not matches:
            return

        type = 0

        # created
        if event.mask & pyinotify.IN_CREATE != 0:
            type |= Event.CREATED_FLAG

        # modified
        if event.mask & pyinotify.IN_MODIFY != 0:
            type |= Event.MODIFIED_FLAG

        # deleted
        if event.mask & pyinotify.IN_DELETE != 0:
            type |= Event.DELETE_FLAG
            del self.matches[event.pathname]

        # enqueue
        for channel_thd, src in matches:
            channel_thd.events.put(Event(event.pathname, type, src))


def monitor(channels, paths, tracking=None, callback=None):
    """
    """
    channel_thds = []
    for channel in channels:
        logger.info('spawning thread for channel "%s"', channel.name)
        channel_thd = ChannelThread(channel, tracking, forever=True)
        channel_thd.start()
        channel_thds.append(channel_thd)

    mask = (pyinotify.IN_MODIFY |
            pyinotify.IN_ATTRIB |
            pyinotify.IN_MOVED_FROM |
            pyinotify.IN_MOVED_TO |
            pyinotify.IN_CREATE |
            pyinotify.IN_DELETE |
            pyinotify.IN_DELETE_SELF |
            pyinotify.IN_MOVE_SELF)
    wm = pyinotify.WatchManager()
    me = _MonitorEvent(channel_thds)
    notifier = pyinotify.Notifier(wm, default_proc_fun=me)
    notifier.coalesce_events(True)
    for path in paths:
        path = path.strip()
        wm.add_watch(path, mask, rec=True, auto_add=True)
        logger.info('monitoring %s', path)
    logger.info('enter notification loop')
    if callback:
        def notifier_callback(notifier):
            return callback()
    else:
        notifier_callback = None
    notifier.loop(callback=notifier_callback)
    logger.info('exit notification loop')

    for channel_thd in channel_thds:
        logger.info('stopping thread for channel "%s"', channel_thd.channel.name)
        channel_thd.stop()
    while channel_thds:
        channel_thds[0].join(1.0)
        if not channel_thds[0].is_alive():
            logger.info('channel "%s" thread has stopped', channel_thd.channel.name)
            channel_thds.pop(0)
