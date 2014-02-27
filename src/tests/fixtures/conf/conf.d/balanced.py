"""
"""
import collections
import re

import slurp


access = re.compile(r"""
# ipv4 address
(?P<ip>\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\s+

# reserved
-\s+

# user
(?:(?P<user>\w+)|-)\s+

# timestamp
\[(?P<timestamp>\d{2}\/\w{3}\/\d{4}:\d{2}:\d{2}:\d{2})\]\s+

# request
"(?:(?P<method>\w+)\s+(?P<uri>.+)\s+HTTP\/(?P<version>\d\.\d)|-)"\s+

# status code
(?:(?P<status>\d+)|-)\s+
 
#  bytes sent
(?:(?P<bytes>\d+)|-)\s+
 
# referrer
"(?:(?P<referrer>.*?)|-)"\s+
 
# user agent
"(?P<user_agent>.*?)"
 
# extras
(?:\s+(?:
request_time_seconds=(?:(?P<request_time_secs>\d+)|-)|
request_time_microseconds=(?:(?P<request_time_usecs>\d+)|-)|
guru_id=(?:(?P<guru_id>\w+)|-)|
\w+=.+?))*
""", re.VERBOSE)


class AccessPayload(slurp.form.Form):
    
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

        
class Access(slurp.form.Form):
    
    src_file = slurp.form.String('path').from_context()
    
    offset_b = slurp.form.Integer('offset.begin').from_context()

    offset_e = slurp.form.Integer('offset.end').from_context()

    timestamp = slurp.form.Datetime(format='DD/MMM/YYYY:HH:mm:ss')
    
    payload = slurp.form.SubForm(AccessPayload, None)


class AccessSearch(slurp.form.Form):

    index = slurp.form.String().format(
        'logs_{year:02}{month:02}{day:02}',
        year='document.timestamp.year',
        month='document.timestamp.month',
        day='document.timestamp.day',
    )

    type = slurp.form.String().constant('application_access')

    document = slurp.form.Field(None)


# sentry

sentry =  re.compile(r"""
\<(?P<facility>\d+).(?P<severity>\w+)\>\s+
(?P<timestamp>\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}\+\d{2}:\d{2})
(?P<host>.+?)\s+
(?P<tag>.+?)(?:\[(?P<process>\d+)\])?:\s+
(?P<project>\d+):\s+
(?P<message>.*)
""", re.VERBOSE)


class Sentry(slurp.form.Form):

    project = slurp.form.String()

    message = slurp.form.String()
