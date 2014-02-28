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

ctx = pilo.ctx

Form = pilo.Form

Field = pilo.Field

SubForm = pilo.fields.SubForm

class Datetime(pilo.fields.Datetime):

    def __init__(self, *args, **kwargs):
        self.format = kwargs.pop('format')
        super(Datetime, self).__init__(*args, **kwargs)

    def _parse(self, value):
        value = self.ctx.src.parse(self.src, value, basestring)
        try:
            return arrow.get(value, self.format).datetime
        except arrow.parser.ParserError, ex:
            self.ctx.errors.invalid(self, str(ex))
            return pilo.ERROR


String = pilo.fields.String

Integer = pilo.fields.Integer

Float = pilo.fields.Float

Dict = pilo.fields.Dict
