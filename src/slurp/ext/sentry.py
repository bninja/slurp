"""
"""
import raven

from .. import settings, Settings, Sink


class SentrySettings(Settings):

    dsns = settings.Dict(settings.String(), settings.String(), 'dsn')

    ignore_unknown = settings.Boolean(default=False)


class Sentry(Sink):

    settings = SentrySettings

    def __init__(self, name, dsns, ignore_unknown):
        super(Sentry, self).__init__(name)
        self.clis = dict([
            (project, raven.Client(dsn)) for project, dsn in dsns.iteritems()
        ])
        self.ignore_unknown = ignore_unknown

    def __call__(self, form, block):
        if form['project'] not in self.clis:
            if self.ignore_unknown:
                return
            raise ValueError(
                'Unknown project {0}, expected one of {1}'.format(form['project'], self.clis.keys())
            )
        self.clis[form['project']].send_encoded(form['message'])
