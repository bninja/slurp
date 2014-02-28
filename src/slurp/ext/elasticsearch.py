"""
"""
import pyes

from .. import settings, Sink


class ElasticSearchSettings(settings.Form):

    connections = settings.List(settings.String())

    timeout = settings.Integer(default=10).min(0)

    batch_size = settings.Integer(default=0).min(0)


class ElasticSearch(Sink):

    settings = ElasticSearchSettings

    def __init__(self, name, connections, timeout, batch_size):
        super(ElasticSearch, self).__init__(name)
        self._es = None
        self.connections = connections
        self.timeout = timeout
        self.batch_size = batch_size
        self.forms = []

    @property
    def es(self):
        if self._es is None:
            self._es = pyes.ES(self.connections, timeout=self.timeout)
        return self._es

    def __call__(self, form, offset):
        self.es.index(
            doc=form['document'],
            index=form['index'],
            doc_type=form['type'],
            id=form.get('id', None),
        )
        return offset

    def flush(self):
        offset = None
        while self.forms:
            form, offset = self.forms.pop()
            self.es.index(
                doc=form['document'],
                index=form['index'],
                doc_type=form['type'],
                id=form.get('id', None),
            )
        return offset
