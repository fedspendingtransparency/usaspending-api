import logging

from django.http import HttpResponse
from django.test import RequestFactory

from usaspending_api.common.logging import LoggingMiddleware


def test_interleaved_requests_keep_their_own_log_lines(caplog):
    """Two requests in flight at once (threads or ASGI) must not share logging state."""
    middleware = LoggingMiddleware(lambda request: HttpResponse())
    factory = RequestFactory()
    request_a = factory.post("/api/v2/a/", data='{"who": "a"}', content_type="application/json")
    request_b = factory.post("/api/v2/b/", data='{"who": "b"}', content_type="application/json")

    with caplog.at_level(logging.INFO, logger="server"):
        middleware.process_request(request_a)
        # request B arrives while A is still inside its view
        middleware.process_request(request_b)
        middleware.process_response(request_a, HttpResponse(status=200))
        middleware.process_response(request_b, HttpResponse(status=201))

    records = [record for record in caplog.records if record.name == "server"]
    assert [(record.path, record.status_code) for record in records] == [("/api/v2/a/", 200), ("/api/v2/b/", 201)]
    assert [record.request for record in records] == ['{"who": "a"}', '{"who": "b"}']


def test_exception_path_uses_the_request_state_too(caplog):
    middleware = LoggingMiddleware(lambda request: HttpResponse())
    request = RequestFactory().get("/api/v2/boom/")

    with caplog.at_level(logging.ERROR, logger="server"):
        middleware.process_request(request)
        middleware.process_exception(request, RuntimeError("boom"))

    (record,) = [record for record in caplog.records if record.name == "server"]
    assert record.path == "/api/v2/boom/"
    assert record.status_code == 500
    assert record.response_ms >= 0
