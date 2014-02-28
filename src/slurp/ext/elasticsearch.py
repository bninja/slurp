"""
"""
import pyes

from .. import settings, Sink


class ElasticSearchSettings(settings.Form):

    connections = settings.List(settings.String())

    timeout = settings.Integer(default=10).min(0)

    bulk = settings.Boolean(default=True)

    bulk_size = settings.Integer(default=400).min(1)


class ElasticSearch(Sink):

    settings = ElasticSearchSettings

    def __init__(self, name, connections, timeout, bulk, bulk_size):
        super(ElasticSearch, self).__init__(name)
        self._es = None
        self.connections = connections
        self.timeout = timeout
        self.bulk = bulk
        self.bulk_size = bulk_size
        self.index = (self.bulk_index if bulk else self.single_index)

    @property
    def es(self):
        if self._es is None:
            self._es = pyes.ES(
                self.connections,
                timeout=self.timeout,
                bulk_size=self.bulk_size,
            )
        return self._es

    def single_index(self, index, doc_type, id, doc):
        self.es.index(
            doc=doc,
            index=index,
            doc_type=doc_type,
            id=id,
            bulk=False,
        )

    def bulk_index(self, index, doc_type, id, doc):
        bulk = self.es.index(
            doc=doc,
            index=index,
            doc_type=doc_type,
            id=id,
            bulk=True,
        )
        # NOTE: bulk == None means is pending in pyes
        return bulk is None

    # Sink

    def __call__(self, form, block):
        return self.index(
            doc=form['document'],
            index=form['index'],
            doc_type=form['type'],
            id=form.get('id', None),
        )

    def flush(self):
        if self.bulk:
            self.es.force_bulk()
