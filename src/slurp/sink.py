from pprint import pprint

from . import settings


class Sink(object):

    settings = None

    def __init__(self, name):
        self.name = name

    def __call__(self, form, offset):
        raise NotImplementedError()

    def flush(self):
        pass


class SinkSettings(settings.Form):

    type = settings.Code().as_class(Sink)


class Echo(Sink):

    def __call__(self, form, offset):
        pprint(form)
        return offset
