from ddtrace import tracer


def add_headers(get_response):
    def middleware(request):
        span = tracer.current_root_span()
        if span:
            for header in request.headers:
                span.set_tag("http.headers.%s" % header, request.headers[header])
        response = get_response(request)
        return response

    return middleware
