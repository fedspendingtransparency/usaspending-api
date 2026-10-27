import pytest
from opensearchpy import NotFoundError, TransportError

from usaspending_api.common.elasticsearch.search_wrappers import Search, TransactionSearch
from usaspending_api.common.exceptions import ElasticsearchQueryComplexityException


def _transport_error(status: int, error_type: str, reason: str) -> TransportError:
    """Build a TransportError shaped like an OpenSearch search_phase_execution_exception body."""
    info = {
        "error": {
            "type": "search_phase_execution_exception",
            "reason": "all shards failed",
            "caused_by": {"type": error_type, "reason": reason},
        },
        "status": status,
    }
    return TransportError(status, "search_phase_execution_exception", info)


@pytest.mark.parametrize(
    "error, expected",
    [
        (_transport_error(400, "too_many_clauses", "abc"), True),
        (_transport_error(400, "too_many_nested_clauses", "abc"), True),
        (_transport_error(400, "runtime_exception", "maxClauseCount is set to 1024"), True),
        (_transport_error(400, "parsing_exception", "unknown query [foo]"), False),
        (_transport_error(500, "illegal_state_exception", "abc"), False),
        (TransportError(503, "service unavailable", None), False),
        (TransportError(500, "no caused_by", {"error": {"type": "x"}}), False),
        (TransportError("N/A", "connection error", Exception("socket closed")), False),
    ],
)
def test_is_too_many_clauses_error(error, expected):
    assert Search._is_too_many_clauses_error(error) is expected


def test_handle_execute_raises_complexity_exception(monkeypatch):
    original = _transport_error(400, "too_many_nested_clauses", "maxClauseCount is set to 1024")

    def raise_error(self, timeout):
        raise original

    monkeypatch.setattr(TransactionSearch, "_execute", raise_error)

    with pytest.raises(ElasticsearchQueryComplexityException) as exc_info:
        TransactionSearch().handle_execute(retries=1)

    assert exc_info.value.__cause__ is original


def test_handle_execute_reraises_other_transport_errors(monkeypatch):
    original = _transport_error(400, "parsing_exception", "unknown query [foo]")

    def raise_error(self, timeout):
        raise original

    monkeypatch.setattr(TransactionSearch, "_execute", raise_error)

    with pytest.raises(TransportError) as exc_info:
        TransactionSearch().handle_execute(retries=1)

    assert exc_info.value is original
    assert not isinstance(exc_info.value, ElasticsearchQueryComplexityException)