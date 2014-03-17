"""
Sink for sending form(s) to email. Typical usage is:

.. code:: text

    [sink:nginx-email]
    type = Email
    host = 'smtp.exmaple.org'
    port = 25
    to = me@example.org
    rollup = true
    template =

        % for form in forms[:25]:
        {form.timestamp} [{form.severity}] {form.payload.message}
        % endfor
        % if len(forms) > 25:
        ... and {len(forms) - 25} more
        % endif

Here template is an embedded `mako <http://www.makotemplates.org/>`_ template,
but could also be pulled out to a file if you prefer:

.. code:: text

    [sink:nginx-email]
    type = Email
    host = 'smtp.exmaple.org'
    port = 25
    to = me@example.org
    rollup = true
    template = /etc/slurp/conf.d/nginx-email.mako

The template has access to:

- ``slurp``, the slurp module
- ``sink``, the sink instance
- ``forms``, the forms passed to the sink

"""
from __future__ import absolute_import

import email.mime.text
import logging
import os
import socket
import smtplib

import mako.exceptions
import mako.lookup
import mako.template

import slurp

from .. import settings, Settings, Sink


logger = logging.getLogger(__name__)


class Template(settings.String):

    def _parse(self, value):
        parsed = super(Template, self)._parse(value)
        try:
            if parsed.count('\n') > 0:
                return self._inline(parsed)
            return self._load(parsed)
        except Exception, ex:
            self.ctx.errors.invalid(str(ex))
            return settings.ERROR

    def _inline(self, raw):
        return mako.template.Template(raw)

    def _load(self, path):
        dir_path = os.path.dirname(path)
        file_name = os.path.basename(path)
        lookup = mako.lookup.TemplateLookup(directories=[dir_path])
        return lookup.get_template(file_name)


class EmailSettings(Settings):

    #: Either a path to a mako  template file or an in-line mako template.
    template = Template()

    #: Flag indicating whether email should be a rollup.
    rollup = settings.Boolean(default=False)

    #: Where it comes from.
    from_address = settings.String(
        'from', default='slurp@{0}'.format(socket.gethostname())
    )

    #: List of who to send it to.
    to_addresses = settings.List(settings.String(), 'to')

    #: Server host.
    host = settings.String(default='127.0.0.1')

    #: Server port. Usually 25 or 587.
    port = settings.Integer(default=25)

    #: Send timeout in seconds.
    timeout = settings.Integer(default=60)

    #: A (user name, password) tuple.
    creds = settings.Tuple((settings.String(), settings.String()), default=None)


class Email(Sink):

    settings = EmailSettings

    def __init__(self, name, template, rollup, from_address, to_addresses, host, port, timeout, creds):
        super(Email, self).__init__(name)
        self.template = template
        self.rollup = rollup
        self.from_address = from_address
        self.to_addresses = to_addresses
        self.host = host
        self.port = port
        self.timeout = timeout
        self.creds = creds
        self.forms = []
        self.blocks = []

    def render(self, forms, blocks):
        try:
            return self.template.render(slurp=slurp, sink=self, forms=forms, blocks=blocks)
        except:
            logger.exception(mako.exceptions.text_error_template().render())
            raise

    def msg(self, forms, blocks):
        text = self.render(forms, blocks)
        msg = email.mime.text.MIMEText(text)
        msg['Subject'] = self.name
        msg['From'] = self.from_address
        msg['To'] = ' '.join(self.to_addresses)
        return msg

    def send(self, msg):
        cxn = smtplib.SMTP(self.host, self.port, timeout=self.timeout)
        try:
            if self.creds:
                cxn.login(*self.creds)
            cxn.sendmail(self.from_address, self.to_addresses, msg.as_string())
        finally:
            cxn.quit()

    # Sink

    def __call__(self, form, block):
        if self.rollup:
            self.forms.append(form)
            self.blocks.append(block)
            return True
        self.send(self.msg([form], [block]))

    def flush(self):
        if not self.rollup:
            return
        if self.forms:
            forms = self.forms
            self.forms = []
            blocks = self.blocks
            self.blocks = []
            self.send(self.msg(forms, blocks))
