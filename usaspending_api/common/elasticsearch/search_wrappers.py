import logging
from ssl import CERT_NONE
from typing import Callable, Optional, Union

from django.conf import settings
from elasticsearch import ConnectionError, ConnectionTimeout, Elasticsearch, NotFoundError, TransportError
from elasticsearch.connection import create_ssl_context
from elasticsearch_dsl import Search as SearchBase
from elasticsearch_dsl.response import Response

logger = logging.getLogger("console")


class Search(SearchBase):
    _index_name = None

    def __init__(self, **kwargs) -> None:
        client = self._create_es_client()
        kwargs.update({"index": self._index_name, "using": client})
        super().__init__(**kwargs)

    @staticmethod
    def _create_es_client() -> Elasticsearch:
        if settings.ES_HOSTNAME is None or settings.ES_HOSTNAME == "":
            logger.error("env var 'ES_HOSTNAME' needs to be set for Elasticsearch connection")
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

            return Elasticsearch(**es_config)
        except Exception as e:
            logger.error("Error creating the elasticsearch client: {}".format(e))

    def _execute(self, timeout: str):
        return self.params(timeout=timeout).execute()

    def _count(self, timeout: str):
        return self.count()

    def _handle_retry(self, func: Callable, retries: int, timeout: str) -> Optional[Union[Response, int]]:
        if retries > 20:
            retries = 20
        elif retries < 1:
            retries = 1
        for attempt in range(retries):
            response = func(timeout)
            if response is None:
                logger.info(f"Failure using these: Index='{self._index_name}', Body={self.to_dict()}")
            else:
                return response
        logger.error(f"Unable to reach elasticsearch cluster. {retries} attempt(s) made.")
        return None

    def _handle_errors(self, func: Callable, retries: int, timeout: str) -> Response:
        error_template = "[ERROR] ({type}) with ElasticSearch cluster: {e}"
        try:
            result = self._handle_retry(func, retries, timeout)
        except NameError as e:
            logger.error(error_template.format(type="Hostname", e=str(e)))
            raise
        except (ConnectionError, ConnectionTimeout) as e:
            logger.error(error_template.format(type="Connection", e=str(e)))
            raise
        except NotFoundError as e:
            logger.error(error_template.format(type="404 Not Found", e=str(e)))
            raise
        except TransportError as e:
            logger.error(error_template.format(type="Transport", e=str(e)))
            raise
        except Exception as e:
            logger.error(error_template.format(type="Generic", e=str(e)))
            raise
        return result

    def handle_execute(self, retries: int = 5, timeout: str = "90s") -> Response:
        return self._handle_errors(self._execute, retries, timeout)

    def handle_count(self, retries: int = 5) -> int:
        return self._handle_errors(self._count, retries, None)


class TransactionSearch(Search):
    _index_name = f"{settings.ES_TRANSACTIONS_QUERY_ALIAS_PREFIX}*"

    @staticmethod
    def type_as_string():
        return "transaction_search"


class AwardSearch(Search):
    _index_name = f"{settings.ES_AWARDS_QUERY_ALIAS_PREFIX}*"

    @staticmethod
    def type_as_string():
        return "award_search"


class SubawardSearch(Search):
    _index_name = f"{settings.ES_SUBAWARD_QUERY_ALIAS_PREFIX}*"

    @staticmethod
    def type_as_string():
        return "subaward_search"


class RecipientSearch(Search):
    _index_name = f"{settings.ES_RECIPIENTS_QUERY_ALIAS_PREFIX}*"

    @staticmethod
    def type_as_string():
        return "recipient_search"


class LocationSearch(Search):
    _index_name = f"{settings.ES_LOCATIONS_QUERY_ALIAS_PREFIX}*"

    @staticmethod
    def type_as_string():
        return "location_search"
