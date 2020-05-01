from typing import Union, Optional

import certifi
import logging

from django.conf import settings
from elasticsearch import Elasticsearch
from elasticsearch.connection import create_ssl_context
from ssl import CERT_NONE

from elasticsearch_dsl.response import Response

logger = logging.getLogger("console")
CLIENT = None
ElasticsearchResponse = Optional[Union[dict, Response]]


def instantiate_elasticsearch_client() -> Elasticsearch:
    es_kwargs = {"timeout": 300}

    if "https" in settings.ES_HOSTNAME:
        es_kwargs.update({"use_ssl": True, "verify_certs": True, "ca_certs": certifi.where()})

    return Elasticsearch(settings.ES_HOSTNAME, **es_kwargs)


def create_es_client() -> Elasticsearch:
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
