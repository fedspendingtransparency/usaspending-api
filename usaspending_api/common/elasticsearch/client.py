import logging
import json
from django.conf import settings
from elasticsearch import ConnectionError
from elasticsearch import ConnectionTimeout
from elasticsearch import Elasticsearch
from elasticsearch import NotFoundError
from elasticsearch import TransportError

from usaspending_api.common.elasticsearch.mock_elasticsearch import MockElasticSearch

logger = logging.getLogger("console")
CLIENT_TIMEOUT = settings.ES_TIMEOUT or 15
CLIENT = None


def create_es_client():
    if settings.ES_HOSTNAME is None or settings.ES_HOSTNAME == "":
        logger.error("env var 'ES_HOSTNAME' needs to be set for Elasticsearch connection")
    global CLIENT
    try:
        CLIENT = Elasticsearch(settings.ES_HOSTNAME, timeout=CLIENT_TIMEOUT)
    except Exception as e:
        logger.error("Error creating the elasticsearch client: {}".format(e))


def mock_es_client():
    global CLIENT
    CLIENT = MockElasticSearch()


def es_client_query(index, body, timeout="1m", retries=1):
    if CLIENT is None:
        create_es_client()
    if CLIENT is None:  # If CLIENT is still None, don't even attempt to connect to the cluster
        retries = 0
    elif retries > 20:
        retries = 20
    elif retries < 1:
        retries = 1
    for attempt in range(retries):
        response = _es_search(index=index, body=body, timeout=timeout)
        if response is None:
            logger.info("Failure using these: Index='{}', body={}".format(index, json.dumps(body)))
        else:
            return response
    logger.error("Unable to reach elasticsearch cluster. {} attempt(s) made".format(retries))
    return None


def _es_search(index, body, timeout):
    error_template = "[ERROR] ({type}) with ElasticSearch cluster: {e}"
    result = None
    try:
        result = CLIENT.search(index=index, body=body, timeout=timeout)
    except NameError as e:
        logger.error(error_template.format(type="Hostname", e=str(e)))
    except (ConnectionError, ConnectionTimeout) as e:
        logger.error(error_template.format(type="Connection", e=str(e)))
    except TransportError as e:
        logger.error(error_template.format(type="Transport", e=str(e)))
    except NotFoundError as e:
        logger.error(error_template.format(type="404 Not Found", e=str(e)))
    except Exception as e:
        logger.error(error_template.format(type="Generic", e=str(e)))
    return result
