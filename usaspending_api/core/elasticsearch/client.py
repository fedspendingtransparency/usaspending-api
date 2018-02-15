import logging
from django.conf import settings
from elasticsearch import Elasticsearch

logger = logging.getLogger('console')
CLIENT = Elasticsearch(settings.ES_HOSTNAME)


def es_client_query(index, body, timeout='1m', retries=1):
    if retries > 20:
        retries = 20
    elif retries < 1:
        retries = 1
    for attempt in range(retries):
        try:
            response = CLIENT.search(index=index, body=body, timeout=timeout)
        except Exception:
            logger.exception('There was an error connecting to the ElasticSearch cluster.')
            continue
        return response
    logger.error('Error connecting to elasticsearch. {} attempts made'.format(retries))
    return None
