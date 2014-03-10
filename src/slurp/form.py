"""
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
