"""
Forms are used to translate dictionaries and look like this:

    .. code:: python

        import slurp

        class Form(slurp.Form):

            ip = slurp.form.String()
            user = slurp.form.String(default=None)
            timestamp = slurp.form.Datetime(format='DD/MMM/YYYY:HH:mm:ss')
            method = slurp.form.String(default=None)
            uri = slurp.form.String(default=None)
            version = slurp.form.String(default=None)
            status = slurp.form.Integer(default=None)
            bytes = slurp.form.Integer(default=0).tag('idk')

Use them like this:

    .. code:: python

        from pprint import pprint

        form = Form({
            'ip': '127.0.0.1',
            'timestamp': '20/Feb/2014:11:37:58',
            'method': 'POST',
            'uri': '/bank_accounts/BA3sCBsRa9KvqHqZnnV2n6UC/credits',
            'version': '1.1',
            'status': '201',
            'bytes': '930',
        })
        print form.ip
        print form['ip']
        pprint(form)
        pprint(form.filter('idk', inv=True))

See `pilo <https://github.com/bninja/pilo>`_.
"""
import arrow
import pilo

__all__ = [
    'ctx',
    'Datetime',
    'Float',
    'Form',
    'Integer',
    'inline',
    'String',
    'SubForm',
]

NONE = pilo.NONE

ERROR = pilo.ERROR

IGNORE = pilo.IGNORE

ctx = pilo.ctx

Form = pilo.Form

Field = pilo.Field

SubForm = pilo.fields.SubForm

class Datetime(pilo.fields.String):

    def __init__(self, *args, **kwargs):
        self.format = kwargs.pop('format')
        super(Datetime, self).__init__(*args, **kwargs)

    def _parse(self, value):
        value = self.ctx.src_path.primitive(basestring)
        try:
            return arrow.get(value, self.format).datetime
        except arrow.parser.ParserError, ex:
            self.ctx.errors.invalid('{0} for "{1}"'.format(str(ex), self.format))
            return pilo.ERROR


String = pilo.fields.String

Integer = pilo.fields.Integer

Float = pilo.fields.Float

Dict = pilo.fields.Dict
