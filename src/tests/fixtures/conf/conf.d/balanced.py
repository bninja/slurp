import slurp


class AccessPayload(slurp.Form):

    ip = slurp.form.String()

    user = slurp.form.String(default=None)

    method = slurp.form.String(default=None)

    uri = slurp.form.String(default=None)

    version = slurp.form.String(default=None)

    status = slurp.form.Integer(default=None)

    bytes = slurp.form.Integer(default=0)

    referrer = slurp.form.String(default=None)

    user_agent = slurp.form.String(default=None)

    guru_id = slurp.form.String(default=None)

    request_time_secs = slurp.form.Float(default=0).min(0).tag('exclude')

    request_time_usecs = slurp.form.Float(default=0).min(0).tag('exclude')

    request_time = slurp.form.Float(default=None)

    @request_time.compute
    def request_time(self):
        if any([self.request_time_secs is None, self.request_time_usecs is None]):
            return slurp.form.NONE
        return float('%d.%06d' % (
            self.request_time_secs, self.request_time_usecs
        ))


class Access(slurp.Form):

    src_file = slurp.form.String('block.path').from_context()

    offset_b = slurp.form.Integer('block.begin').from_context()

    offset_e = slurp.form.Integer('block.end').from_context()

    timestamp = slurp.form.Datetime(format='DD/MMM/YYYY:HH:mm:ss')

    payload = slurp.form.SubForm(AccessPayload, None)


class AccessSearch(slurp.Form):

    index = slurp.form.String().format(
        'logs_{year:02}{month:02}{day:02}',
        year='document.timestamp.year',
        month='document.timestamp.month',
        day='document.timestamp.day',
    )

    type = slurp.form.String().constant('application_access')

    document = slurp.form.Field(None)
