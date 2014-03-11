"""
A `Sink` is linked to `Source`s by a `Channel` and is what receives parsed and
formatted blocks. They can either immediately process a received block:

.. code:: python

    class SingleSink(slurp.Sink):

        ...

        def __call__(self, form, block):
            self.cxn.index(form)

or buffer them for later bulk processing.

.. code:: python

    class BulkSink(slurp.Sink):

        ...

        def __call__(self, form, block):
            self.buffer.append(form)
            return True

        def flush(self):
            buffer = self.buffer
            self.buffer = []
            self.cxn.index(*buffer)

To add a new `Sink` you'll typically create a python file similar to this:

.. code:: python

    import crazee
    import slurp

    class MySettings(slurp.Settings):

        connections = slurp.settings.List(slurp.settings.String()).min(1)

        creds = slurp.settings.Tuple(slurp.settings.String(), slurp.settings.String(), default=None)

        retry = slurp.settings.Integer(defalut=None).min(1).max(10)


    class MySink(slurp.Sink):

        settings = MySettings

        def __init__(self, name, connections, creds, retry):
            super(Sentry, self).__init__(name)
            self.cxn = crazee.Connection(connections, creds, retry)

        def __call__(self, form, block):
            self.cxn.tap(form)


and register it as an extension (i.e. put it in /etc/slurp/conf.d/). Of course
these can be embedded in configuration files directly if you prefer.

"""
from pprint import pprint

from . import settings, Settings


class Sink(object):
    """
    The terminal end of a `Channel` for consuming parsed and formatted blocks
    as extract from a file by a `Source`.
    """

    settings = None

    def __init__(self, name):
        """
        :param name: A unique name for the sink.
        """
        self.name = name

    def __call__(self, form, block):
        """
        Called by a channel to send a parsed and formatted block to whatever
        this sink represents. Just raise an exception and the `Channel` will
        deal with it if something bad happens.

        :param form:
            A `dict` like object that has been parsed and formatted
            form the original block.
        :param block:
            The raw block extracted from a file by a `Source`.

        :return:
            True if the block is pending (i.e. has been buffered) otherwise the
            channel will assume it has been successfully processed.

            Note that and previously pending blocks send to this sink will be
            assumed processed any time True is returned

        """
        raise NotImplementedError()

    def flush(self):
        """
        Called to flush all pending parsed and formatted blocks that have been
        send to this sink. The `Channel` will call this whenever it need to.
        Just raise an exception and the `Channel` will deal with it if
        something bad happens.
        """
        pass


class SinkSettings(Settings):

    type = settings.Code().as_class(Sink)


class Echo(Sink):

    def __call__(self, form, block):
        pprint(form)


class Drop(Sink):

    def __call__(self, form, block):
        return True  # NOTE: True means pending


class Tally(Sink):

    def __init__(self, name):
        self.name = name
        self.tally = {
            'bytes': 0,
            'count': 0,
        }

    def __call__(self, form, block):
        self.tally['bytes'] += block.end - block.begin
        self.tally['count'] += 1
        return True  # NOTE: True means pending

    def flush(self):
        print 'bytes:', self.tally['bytes']
        print 'count:', self.tally['count']
        self.tally = {
            'bytes': 0,
            'count': 0,
        }
