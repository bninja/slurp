"""
"""
import logging

try:
    import pyinotify
except ImportError:
    pyinotify = None

from . import settings, form
from .block import Block, Blocks
from .settings import Settings
from .form import Form
from .sink import Sink, SinkSettings, Echo, Drop, Tally
from .source import Source, SourceSettings
from .channel import Channel, ChannelSource, ChannelSettings, ChannelEvent
from .config import Config

__version__ = '0.6.2'

__all__ = [
    'settings',
    'Settings',
    'form',
    'Form',
    'Block',
    'Blocks',
    'Sink',
    'SinkSettings',
    'Echo',
    'Drop',
    'Tally',
    'Source',
    'SourceSettings',
    'Channel',
    'ChannelEvent',
    'ChannelSource',
    'ChannelSettings',
    'Config',
    'touch',
    'tell',
    'reset',
    'consume',
    'watch',
]

logger = logging.getLogger(__name__)


def touch(file_paths, channels):
    """
    Updates and returns consume progress information (i.e. offsets) for files.
    """
    for path in file_paths:
        for channel in channels:
            source = channel.match(path)
            if not source:
                continue
            yield channel.name, source.name, path, source.touch(path)


def tell(file_paths, channels):
    """
    Returns consume progress information (i.e. offsets) for files.
    """
    for path in file_paths:
        for channel in channels:
            source = channel.match(path)
            if not source:
                continue
            yield channel.name, source.name, path, source.tell(path)


def reset(file_paths, channels):
    """
    Resets and returns consume progress information (i.e. offsets) for files.
    """
    for path in file_paths:
        for channel in channels:
            source = channel.match(path)
            if not source:
                continue
            source.reset(path)
            yield channel.name, source.name, path


def consume(file_paths, channels, reset=False):
    """
    Consumes blocks from files.

    :param file_paths:

        Sequence of file paths. A file path can be:

        - a path to a file on disk
        - a (source name, file-like object) tuple
        - a file-like object

        If you specify a file-like object without a source (the third option) then
        there can be only one source associated with channels as otherwise the
        source is ambiguous.

    :param channels:
        List of `Channels` to match against. If a file path does **not** match
        a `Channel` blocks from that file will not be sent to it.

    :param reset:
        Flag indicating whether existing offset information should be reset
        when consuming blocks.
    """
    for path in file_paths:
        matches = 0
        for channel in channels:
            # file path
            if isinstance(path, basestring):
                source = channel.match(path)
                if not source:
                    continue
                matches += 1
            # source name, file path
            elif isinstance(path, tuple):
                source_name, path = path
                for source in channel.sources:
                    if source.name == source_name:
                        break
                else:
                    continue
                matches += 1
            # file-like
            else:
                if len(channel.sources) > 1:
                    raise ValueError(
                       'Cannot determine channel {0} source for {1}'.format(channel, path)
                    )
                source = channel.sources[0]
                matches += 1

            # reset it
            if reset:
                source.reset(path)

            # eat it
            count, bytes, errors, delta = source.consume(path)
            path_name = path if isinstance(path, basestring) else getattr(path, 'name', '<memory>')
            yield channel.name, source.name, path_name, count, bytes, errors

        logger.debug('"%s" matched %s channel(s)', path, matches)


if pyinotify:

    class WatchEvent(pyinotify.ProcessEvent):

        def __init__(self, channels):
            super(WatchEvent, self).__init__()
            self.workers = [channel.worker() for channel in channels]
            for worker in self.workers:
                worker.daemon = True
                worker.start()
            self.matches = {}

        def match(self, path):
            if path not in self.matches:
                matches = []
                for worker in self.workers:
                    if worker.match(path) is not None:
                        matches.append(worker)
                self.matches[path] = matches or None
            return self.matches[path]

        # handlers

        def on_create_file(self, path):
            workers = self.match(path)
            if workers:
                for worker in workers:
                    worker.enqueue(ChannelEvent.create(path))

        def on_modify_file(self, path):
            workers = self.match(path)
            if workers:
                for worker in workers:
                    worker.enqueue(ChannelEvent.modify(path))

        def on_delete_file(self, path):
            workers = self.match(path)
            if workers:
                for worker in workers:
                    worker.enqueue(ChannelEvent.delete(path))
                self.matches.pop(path, None)

        def on_delete_directory(self, path):
            for key in self.matches.keys():
                if key.startswith(path):
                    self.matches.pop(path)

        # pyinotify.ProcessEvent

        def process_default(self, event):
            logger.debug('processing event %s', event)
            path = event.pathname

            if event.mask & pyinotify.IN_ISDIR != 0:
                if event.mask & pyinotify.IN_DELETE != 0:
                    self.on_delete_directory(path)
            else:
                if event.mask & pyinotify.IN_DELETE != 0:
                    self.on_delete_file(path)
                if event.mask & pyinotify.IN_CREATE != 0:
                    self.on_create_file(path)
                else:
                    self.on_modify_file(path)


def watch(paths, channels, recursive=True, auto_add=True):
    """
    Monitors paths (files or directories) for changes to files and consumes
    blocks from them when changes are detected.
    """
    if not pyinotify:
        raise RuntimeError('Cannot import pyinotify, pip install pyinotify!')
    mask = (
        pyinotify.IN_MODIFY |
        pyinotify.IN_ATTRIB |
        pyinotify.IN_MOVED_FROM |
        pyinotify.IN_MOVED_TO |
        pyinotify.IN_CREATE |
        pyinotify.IN_DELETE |
        pyinotify.IN_DELETE_SELF |
        pyinotify.IN_MOVE_SELF
    )
    wm = pyinotify.WatchManager()
    we = WatchEvent(channels)
    notifier = pyinotify.Notifier(wm, default_proc_fun=we)
    notifier.coalesce_events(True)
    for path in paths:
        wm.add_watch(path, mask, rec=recursive, auto_add=auto_add)
        logger.info('watching "%s"', path)
    logger.info('enter notification loop')
    notifier.loop()
    logger.info('exit notification loop')
