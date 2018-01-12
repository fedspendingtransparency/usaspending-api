from django.conf import settings
from django.http import JsonResponse
from django.views import debug
from django.http import Http404
from django import http
from django.utils.timezone import now
from django.utils.deprecation import MiddlewareMixin

from rest_framework import status

import sys
import logging
import traceback


def get_remote_addr(request):
    ip_address = request.META.get("HTTP_X_FORWARDED_FOR", None)
    if ip_address:
        # X_FORWARDED_FOR returns client1, proxy1, proxy2,...
        ip_address = [x.strip() for x in ip_address.split(",")][0]
    else:
        ip_address = request.META.get("REMOTE_ADDR", "")

    return ip_address


class LoggingMiddleware(MiddlewareMixin):
    server_logger = logging.getLogger("server")

    start = None
    log = None
    response_msg = None
    log_response_body = False

    def process_request(self, request):
        self.start = now()

        self.log = {
            "requested_at": now(),
            "path": request.path,
            "remote_addr": get_remote_addr(request),
            "host": request.get_host(),
            "method": request.method
        }

        try:
            # When request body is returned  as <class 'bytes'>
            self.log["request"]=getattr(request, '_body', request.body).decode('ASCII')
        except UnicodeDecodeError:
            self.log["request"] = getattr(request, '_body', request.body)

    def process_response(self, request, response):
        status_code = response.status_code

        self.log["status_code"] = status_code
        self.log["response_ms"] = self.get_response_ms()
        self.log["user"] = request.user

        # In case we want to show the response body in logs for debugging purposes
        # Should not be turned on in production
        if self.log_response_body:
            try:
                # When response is returned  as <class 'bytes'>
                self.response_msg = getattr(response, 'content').decode('ASCII')
            except UnicodeDecodeError:
                self.response_msg = getattr(response, 'content')
        else:
            self.response_msg = " "

        if 100 <= status_code < 400:
            self.server_logger.info(self.response_msg, extra=self.log)
        elif status_code == 404:
            self.handle_404(request, Http404)
        elif 400 <= status_code < 500:
            self.server_logger.warning(self.response_msg, extra=self.log)
        else:
            # 500 or greater messages will be processed by the process_exception function
            pass

        return response

    def process_exception(self, request, exception):
        """Get the exception info now, in case another exception is thrown later."""

        if isinstance(exception, http.Http404):
            return self.handle_404(request, exception)
        else:
            return self.handle_500(request, exception)

    @staticmethod
    def production_response(exception, status_type):
        """Return an HttpResponse that displays a friendly error message."""
        error_response = {"message": str(exception)}

        if status_type == 404:
            status_code = status.HTTP_404_NOT_FOUND
        else:
            status_code = status.HTTP_500_INTERNAL_SERVER_ERROR

        return JsonResponse(error_response, status=status_code)

    def get_response_ms(self):
        duration = now() - self.start
        return int(duration.total_seconds() * 1000)

    def handle_404(self, request, exception):

        self.log["status_code"] = 404
        self.log["response_ms"] = self.get_response_ms()
        self.log["user"] = request.user

        if not self.response_msg:
            self.response_msg = str(exception)

        self.server_logger.warning(self.response_msg, extra=self.log)

    def handle_500(self, request, exception):
        exc_info = sys.exc_info()

        traceback_str = traceback.format_exc()

        self.log["status_code"] = 500
        self.log["response_ms"] = self.get_response_ms()
        self.log["user"] = request.user
        self.server_logger.error("%s", traceback_str, extra=self.log)

