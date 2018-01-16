from django.http import Http404
from django import http
from django.utils.timezone import now
from django.utils.deprecation import MiddlewareMixin

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

    def process_request(self, request):
        """Func called when a request is called on server, function stores request fields for logging"""
        self.start = now()

        self.log = {
            "path": request.path,
            "remote_addr": get_remote_addr(request),
            "host": request.get_host(),
            "method": request.method
        }

        try:
            # When request body is returned  as <class 'bytes'>
            self.log["request"] = getattr(request, '_body', request.body).decode('ASCII')
        except UnicodeDecodeError:
            self.log["request"] = getattr(request, '_body', request.body)

    def process_response(self, request, response):
        """
        Func gets called before reponse is returned from server, stores response fields and logs
        request and response to server.log
        """

        status_code = response.status_code

        self.log["status_code"] = status_code
        self.log["response_ms"] = self.get_response_ms()
        self.log["traceback"] = None

        if 100 <= status_code < 400:
            self.log["status"] = 'INFO'
            self.log["timestamp"] = now().strftime('%d/%m/%y %H:%M:%S')
            self.server_logger.info(self.get_message_string(), extra=self.log)
        elif status_code == 404:
            self.handle_404(request, Http404)
        elif 400 <= status_code < 500:
            self.log["status"] = 'WARNING'
            self.log["timestamp"] = now().strftime('%d/%m/%y %H:%M:%S')
            self.server_logger.warning(self.get_message_string(), extra=self.log)
        else:
            # 500 or greater messages will be processed by the process_exception function
            pass

        return response

    def get_message_string(self):
        """Returns logging info as string for message"""
        return "[{timestamp}] [{status}] [{method}] [{path} : {status_code}]" \
               " [{remote_addr}] [{host}] [{response_ms}]".format(**self.log)

    def process_exception(self, request, exception):
        """
        Get the exception info now, in case another exception is thrown later.
        Logs exception as an error
        """

        if isinstance(exception, http.Http404):
            return self.handle_404(request, exception)
        else:
            return self.handle_500(request)

    def get_response_ms(self):
        """Returns time elapsed from request to response/exception"""
        duration = now() - self.start
        return int(duration.total_seconds() * 1000)

    def handle_404(self, request, exception):
        """Logs 404 status"""
        self.log["status_code"] = 404
        self.log["response_ms"] = self.get_response_ms()
        self.log["status"] = 'WARNING'
        self.log["timestamp"] = now().strftime('%d/%m/%y %H:%M:%S')

        self.server_logger.warning(self.get_message_string(), extra=self.log)

    def handle_500(self, request):
        """Logs 500 error"""
        traceback_str = traceback.format_exc()

        self.log["status_code"] = 500
        self.log["response_ms"] = self.get_response_ms()
        self.log["status"] = 'ERROR'
        self.log["timestamp"] = now().strftime('%d/%m/%y %H:%M:%S')
        self.log["traceback"] = traceback_str

        self.server_logger.error("%s", self.get_message_string(), extra=self.log)
