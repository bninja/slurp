import re

import slurp


# access

access = re.compile(r"""
# ipv4 address
(?P<ip>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\s+

# user
(?:(?P<user>\w+)|-)\s+

# timestamp
\[(?P<timestamp>\d{4}\/\w{3}\/\d{2}\:\d{2}\:\d{2}\:\d{2})]\s+

# request
"(?:(?P<method>\w+)\s+(?P<uri>.+)\s+HTTP\/(?P<version>\d\.\d)|-)"\s+

# status code
(?:(?P<status_code>\d+)|-)\s+

#  bytes sent
(?:(?P<bytes_sent>\d+)|-)\s+

# referrer
"(?P<referrer>.*?)"\s+

# user agent
"(?P<user_agent>.*?)"

# extras
(?:\s+(?:
request_time_seconds=(?P<request_time_secs>\d+)|
request_time_microseconds=(?P<request_time_usecs>\d+)|
guru_id=(?P<guru_id>\w+)|
\w+=.+?))*
""", re.VERBOSE)


class AccessPayload(slurp.form.Form):
    
    ip = slurp.form.String()
    
    user = slurp.form.String(default=None)
    
    method = slurp.form.String(default=None)
    
    uri = slurp.form.String(default=None)
    
    version = slurp.form.String(default=None)
    
    status = slurp.form.Integer()
    
    bytes = slurp.form.Integer(default=0)
    
    referrer = slurp.form.String(default=None)

    user_agent = slurp.form.String(default=None)

    guru_id = slurp.form.String(default=None)
    
    request_time_secs = slurp.form.Float().min(0).tag('exclude')
                              
    request_time_usecs = slurp.form.Float().min(0).tag('exclude')
        
    request_time = slurp.form.Float(default=None)
    
    @request_time.resolve
    def request_time(self):
        if any([self.request_time_secs is None, self.request_time_usecs is None]):
            return slurp.form.NONE
        return float('%d.%06d' % (
            self.request_time_secs, self.request_time_usecs,
        )) 

        
class Access(slurp.form.Form):
    
    src_file = slurp.form.String('src.path')
    
    offset_b = slurp.form.Integer('src.offset.begin')

    offset_e = slurp.form.Integer('src.offset.end')

    timestamp = slurp.form.Datetime(format='YYYY/MM/DD:HH:mm:ss')
    
    payload = slurp.form.SubForm(AccessPayload, None)


class AccessSearch(slurp.form.Form):
    
    index = slurp.form.String().format(
        'logs_{year:02}{month:02}{day:02}',
        year='document.timestamp.year',
        month='document.timestamp.month',
        day='document.timestamp.day',
    )
    
    type = slurp.form.String().constant('nginx_access')
    
    document = slurp.form.Field(None)


# error

error = re.compile(r"""
# timestamp
(?P<timestamp>\d{4}/\d{2}/\d{2}\s+\d{2}:\d{2}:\d{2})\s+

# severity
[(?P<severity>\w+)]\s+

# message
(?P<message>.*)
""", re.VERBOSE)


class ErrorPayload(slurp.form.Form):

    message = slurp.form.String()


class Error(slurp.form.Form):
    
    src_file = slurp.form.String('src.path')
    
    offset_b = slurp.form.Integer('src.offset.begin')

    offset_e = slurp.form.Integer('src.offset.end')

    timestamp = slurp.form.Datetime(format='YYYY/MM/DD HH:MM:SS')
    
    severity = slurp.form.String()
    
    payload = slurp.form.SubForm(ErrorPayload, None)


class ErrorSearch(slurp.form.Form):
    
    index = slurp.form.String().format(
        'logs_{year:02}{month:02}{day:02}',
        year='document.timestamp.year',
        month='document.timestamp.month',
        day='document.timestamp.day',
    )
    
    type = slurp.form.String().constant('nginx_error')
    
    document = slurp.form.Field(None)
