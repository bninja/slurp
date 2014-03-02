from pprint import pprint

from . import settings, Settings


class Sink(object):

    settings = None

    def __init__(self, name):
        self.name = name

    def __call__(self, form, block):
        raise NotImplementedError()

    def flush(self):
        pass


class SinkSettings(Settings):

    type = settings.Code().as_class(Sink)


class Echo(Sink):

    def __call__(self, form, block):
        pprint(form)


class Drop(Sink):

    def __call__(self, form, block):
        # NOTE: True means pending
        return True
