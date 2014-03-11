import slurp

from  . import TestCase


class TestConfig(TestCase):

    def test_source_settings(self):
        includes = [
            self.fixture('conf', 'conf.d', '*.conf'),
            self.fixture('conf', 'conf.d', '*.py'),
        ]
        config = slurp.Config(
            includes=includes,
        )
        settings = config.source_settings('balanced-error')
        self.assertItemsEqual([
            'terminal',
            'form',
            'pattern',
            'filter',
            'strict',
            'prefix',
            'read_size',
            'buffer_size',
            'globs',
        ], settings.keys())
        self.assertIsNotNone(settings.prefix)
        self.assertIsNotNone(settings.terminal)
        self.assertIsNotNone(settings.pattern)

    def test_scan(self):
        includes = [
            self.fixture('conf', 'conf.d', '*.conf'),
            self.fixture('conf', 'conf.d', '*.py'),
        ]
        config = slurp.Config(
            includes=includes,
        )
        self.assertItemsEqual([
            'balanced-access',
            'balanced-error',
            'nginx-error',
            'balanced-sentry',
            'nginx-access',
            ], config.source_names)
        map(config.source_settings, config.source_names)
        map(config.source, config.source_names)
        self.assertItemsEqual([
            'balanced-search',
            'nginx-search',
            'nginx-email',
            'balanced-sentry',
            ], config.sink_names)
        map(config.sink_settings, config.sink_names)
        map(config.sink, config.sink_names)
        self.assertItemsEqual([
            'balanced-access-search',
            'nginx-access-search',
            'nginx-error-search',
            'balanced-sentry',
            ], config.channel_names)
        map(config.channel_settings, config.channel_names)
        map(config.channel, config.channel_names)
