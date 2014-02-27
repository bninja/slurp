import inspect
from pprint import pprint

from . import settings


class SinkSettings(settings.Form):

    type = settings.Code()
    
    @type.validate
    def type(self, value):
        if not inspect.isclass(value):
            self.ctx.errors.invlid(
                self.ctx.field, '"{0}" is not a class'.format(value)
            )
        if not issubclass(value, Sink):
            self.ctx.errors.invlid(
                self.ctx.field, '"{0}" is not a sub-class of slurp.Sink'.format(value)
            )
            return False
        return True


class Sink(object):

    settings = None
    
    def __init__(self, name):
        self.name = name

    def __call__(self, form, offset):
        raise NotImplementedError()
        
    def flush(self):
        pass


class Echo(Sink):

    def __call__(self, form, offset):
        pprint(form)
        return offset
