import certifi
import logging
import json

from django.conf import settings
from elasticsearch import ConnectionError
from elasticsearch import ConnectionTimeout
from elasticsearch import Elasticsearch
from elasticsearch import NotFoundError
from elasticsearch import TransportError
from elasticsearch.connection import create_ssl_context
from ssl import CERT_NONE

logger = logging.getLogger("console")
CLIENT = None


def instantiate_elasticsearch_client():
    es_kwargs = {"timeout": 300}

    if "https" in settings.ES_HOSTNAME:
        es_kwargs.update({"use_ssl": True, "verify_certs": True, "ca_certs": certifi.where()})

    return Elasticsearch(settings.ES_HOSTNAME, **es_kwargs)


def create_es_client():
    if settings.ES_HOSTNAME is None or settings.ES_HOSTNAME == "":
        logger.error("env var 'ES_HOSTNAME' needs to be set for Elasticsearch connection")
    global CLIENT
    es_config = {"hosts": [settings.ES_HOSTNAME], "timeout": settings.ES_TIMEOUT}
    try:
        # If the connection string is using SSL with localhost, disable verifying
        # the certificates to allow testing in a development environment
        # Also allow host.docker.internal, when SSH-tunneling on localhost to a remote nonprod instance over HTTPS
        if settings.ES_HOSTNAME.startswith(("https://localhost", "https://host.docker.internal")):
            logger.warning("SSL cert verification is disabled. Safe only for local development")
            import urllib3

            urllib3.disable_warnings()
            ssl_context = create_ssl_context()
            ssl_context.check_hostname = False
            ssl_context.verify_mode = CERT_NONE
            es_config["ssl_context"] = ssl_context

        CLIENT = Elasticsearch(**es_config)
    except Exception as e:
        logger.error("Error creating the elasticsearch client: {}".format(e))


def es_client_query(index, body, timeout="1m", retries=5):
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
    except NotFoundError as e:
        logger.error(error_template.format(type="404 Not Found", e=str(e)))
    except TransportError as e:
        logger.error(error_template.format(type="Transport", e=str(e)))
    except Exception as e:
        logger.error(error_template.format(type="Generic", e=str(e)))
    return result
