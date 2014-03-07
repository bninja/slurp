import logging

logger = logging.getLogger(__name__)

from ..sink import Echo, Drop, Tally

try:
    from elasticsearch import ElasticSearch
except ImportError, ex:
    logger.warning('unable to load elastic search extension - %s', ex)
except Exception, ex:
    logger.exception('unable to load elastic search extension\n')

try:
    from sentry import Sentry
except ImportError, ex:
    logger.warning('unable to load sentry extension - %s', ex)
except Exception, ex:
    logger.exception('unable to load sentry extension\n')

try:
    from email import Email
except ImportError, ex:
    logger.warning('unable to load email extension - %s', ex)
except Exception, ex:
    logger.exception('unable to load email extension\n')
