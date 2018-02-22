import logging
import json
from django.conf import settings
from elasticsearch import ConnectionError
from elasticsearch import ConnectionTimeout
from elasticsearch import Elasticsearch
from elasticsearch import NotFoundError
from elasticsearch import TransportError
from urllib3.exceptions import LocationValueError

logger = logging.getLogger('console')

try:
    CLIENT = Elasticsearch(settings.ES_HOSTNAME, timeout=15)
except LocationValueError as e:
    logging.error('ES_HOSTNAME is not specified!!!')
except Exception as e:
    logger.exception('Error creating the elasticsearch client')


def es_client_query(index, body, timeout='1m', retries=1):
    if retries > 20:
        retries = 20
    elif retries < 1:
        retries = 1
    for attempt in range(retries):
        response = _es_search(index=index, body=body, timeout=timeout)
        if response is None:
            logger.info('Failure using these: Index=\'{}\', body={}'.format(index, json.dumps(body)))
        else:
            return response
    logger.error('Unable to reach elasticsearch cluster. {} attempt(s) made'.format(retries))
    return None


def _es_search(index, body, timeout):
    error_template = '[ERROR] ({type}) with ElasticSearch cluster: {e}'
    result = None
    try:
        result = CLIENT.search(index=index, body=body, timeout=timeout)
    except (ConnectionError, ConnectionTimeout) as e:
        logger.error(error_template.format(type='Connection', e=str(e)))
    except TransportError as e:
        logger.error(error_template.format(type='Transport', e=str(e)))
    except NotFoundError as e:
        logger.error(error_template.format(type='404 Not Found', e=str(e)))
    except Exception as e:
        logger.error(error_template.format(type='Generic', e=str(e)))
    return result
