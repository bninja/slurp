"""
"""
import logging

try:
    import pyinotify
except ImportError:
    pyinotify = None

from . import settings
from . import form
from .block import Block, Blocks
from .form import Form
from .sink import Sink, SinkSettings, Echo
from .source import Source, SourceSettings
from .channel import Channel, ChannelSource, ChannelSettings
from .config import Config

__version__ = '0.9'

__all__ = [
    'settings',
    'form',
    'Form',
    'Block',
    'Blocks',
    'Sink',
    'SinkSettings',
    'Echo',
    'Source',
    'SourceSettings',
    'Channel', 'ChannelSource',
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
    """
    for path in file_paths:
        for channel in channels:
            source = channel.match(path)
            if not source:
                continue
            print channel.name, source.name, path, source.touch(path)


def tell(file_paths, channels):
    """
    """
    for path in file_paths:
        for channel in channels:
            source = channel.match(path)
            if not source:
                continue
            print channel.name, source.name, path, source.tell(path)


def reset(file_paths, channels):
    """
    """
    for path in file_paths:
        for channel in channels:
            source = channel.match(path)
            if not source:
                continue
            source.reset(path)
            print channel.name, source.name, path, 0


def consume(file_paths, channels):
    """
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
                       'Cannot determine channel {} source for {}'.format(channel, path)
                    )
                source = channel.sources[0]
                matches += 1

            # eat it
            source.consume(path)

        logger.debug('"%s" matched %s channel(s)', path, matches)


if pyinotify:

    class WatchEvent(pyinotify.ProcessEvent):
        """
        """

        def __init__(self, channels):
            super(WatchEvent, self).__init__()
            self.channels = channels
            self.matches = {}

        def on_update_file(self, path):
            if path not in self.matches:
                matches = []
                for channel in self.channels:
                    source = channel.match(path)
                    if source:
                        matches.append(source)
                self.matches[path] = matches or None
            if self.matches[path]:
                for source in self.matches[path]:
                    if source.channel.throttle:
                        continue
                    try:
                        source.consume(path)
                        source.channel.throttle.reset()
                    except Exception:
                        duration = source.channel.throttle()
                        logger.exception(
                            'throttling channel %s for %s sec(s)',
                            source.channel.name, duration,
                        )


        def on_delete_directory(self, path):
            for key in self.matches.keys():
                key.startwith(path)
                self.matches.pop(key, None)

        def on_delete_file(self, path):
            self.matches.pop(path, None)

        def process_default(self, event):
            logger.debug('processing event %s', event)
            path = event.pathname
            delete = (event.mask & pyinotify.IN_DELETE != 0)
            directory = (event.mask & pyinotify.IN_ISDIR != 0)
            if delete:
                if directory:
                    self.on_delete_directory(path)
                else:
                    self.on_delete_file(path)
            elif not directory:
                self.on_update_file(path)



def watch(paths, channels, recursive=True, auto_add=True):
    """
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
