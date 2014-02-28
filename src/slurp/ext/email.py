"""
"""
import mako

from .. import settings, Sink


class EmailSettings(settings.Form):

    template = settings.String()

    rollup = settings.Boolean(default=False)

    host = settings.String(default='127.0.0.1')

    port = settings.Integer(default=25)

    timeout = settings.Integer(default=60)

    creds = settings.Tuple(settings.String(), settings.String(), default=None)


class Email(Sink):

    settings = EmailSettings

    def __init__(self, name, template, rollup, host, port, timeout, creds):
        super(Email, self).__init__(name)
        self.template = template
        self.rollup = rollup
        self.host = host
        self.port = port
        self.timeout = timeout
        self.creds = creds

    def __call__(self, form, block):
        if self.rollup:
            return True
        # TODO

    def flush(self, form, block):
        if not self.rollup:
            return
        # TODO
