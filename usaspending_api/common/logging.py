from django.utils.timezone import now
from django.utils.deprecation import MiddlewareMixin

import logging
import traceback
from time import perf_counter  # Matches response time browsers return more accurately than now()


def get_remote_addr(request):
    """ Get IP address of user making request can be used for other logging"""
    ip_address = request.META.get("HTTP_X_FORWARDED_FOR", None)
    if ip_address:
        # X_FORWARDED_FOR returns client1, proxy1, proxy2,...
        ip_address = [x.strip() for x in ip_address.split(",")][0]
    else:
        ip_address = request.META.get("REMOTE_ADDR", "")

    return ip_address


class LoggingMiddleware(MiddlewareMixin):
    """
    Class uses the MiddlewareMixin to alter Django's input and output system. The following class methods get called
    automatically when the following happens:

    process_request(): Django calls this when a view is requested.
    process_response(): Django calls this when a view returns a response.
    process_exception(): Django calls this when a view raises an exception.

    Desired format laid out in settings.py : {
      "timestamp": "11/01/18 22:52:03", (When the request was made)
      "status": "INFO", (Level name of log)
      "method": "POST", (Request method type)
      "path": "/api/v2/download/count/", (Path request was made from)
      "status_code": "200", (Status response)
      "remote_addr": "127.0.0.1", (IP address where request came from)
      "host": "localhost:8000", (Host name or IP address)
      "response_ms": "848", (Time it took to return a response or exception)
      "message": "[11/01/18 22:52:03] [INFO] [POST] [/api/v2/download/count/ : 200]
                    [127.0.0.1] [localhost:8000] [848]",
      (message is [timestamp] [status] [method] [ path : status_code] [remote_addr] [host] [response_ms]
            this format is used to search through Kibana interface)
      "request": {"filters":{...}} (request body sent by user)
      "traceback": null (traceback of call if error, used for debugging server error)
    }

    LOG Levels:
    * INFO: Logs routine items to highlight progress in an application during regular operation
    * WARNING: When action is important, but not an error, and could be potential harmful to the application
    * ERROR: To log something went wrong in the application

    """

    server_logger = logging.getLogger("server")

    start = None
    log = None

    def process_request(self, request):
        """Func called when a request is called on server, function stores request fields for logging"""
        self.start = perf_counter()

        self.log = {
            "path": request.path,
            "remote_addr": get_remote_addr(request),
            "host": request.get_host(),
            "method": request.method,
            "timestamp": now().strftime("%m/%d/%y %H:%M:%S"),
        }

        try:
            # When request body is returned  as <class 'bytes'>
            self.log["request"] = getattr(request, "_body", request.body).decode("ASCII")
        except UnicodeDecodeError:
            self.log["request"] = getattr(request, "_body", request.body)

    def process_response(self, request, response):
        """
        Func gets called before reponse is returned from server, stores response fields and logs
        request and response to server.log
        """

        status_code = response.status_code

        self.log["status_code"] = status_code
        self.log["response_ms"] = self.get_response_ms()
        self.log["traceback"] = None
        if response._headers:
            if "key" in response._headers and len(response._headers["key"]) >= 2:
                self.log["cache_key"] = response._headers["key"][1]
            if "cache-trace" in response._headers and len(response._headers["cache-trace"]) >= 2:
                self.log["cache_trace"] = response._headers["cache-trace"][1]

        if 100 <= status_code < 400:
            # Logged at an INFO level: 1xx (Informational), 2xx (Success), 3xx Redirection
            self.log["status"] = "INFO"
            self.server_logger.info(self.get_message_string(), extra=self.log)
        elif 400 <= status_code < 500:
            # Logged at an WARNING level: 4xx (Client error)
            self.log["status"] = "WARNING"

            # Get response message
            try:
                # If API returns error message
                self.log["error_msg"] = str(response.data)
            except AttributeError:
                # For cases when Django responds with an error template, get the response content (byte string)
                self.log["error_msg"] = response.getvalue().decode("ASCII")

            # Adding separate error message to message field for user to view error on Kibana default view
            error_msg_str = "[" + self.log["error_msg"] + "]"

            self.server_logger.warning("{} {}".format(self.get_message_string(), error_msg_str), extra=self.log)
        else:
            # 500 or greater messages will be processed by the process_exception function
            pass

        return response

    def get_message_string(self):
        """Returns logging info as string for message"""
        return (
            "[{timestamp}] [{status}] [{method}] [{path} : {status_code}]"
            " [{remote_addr}] [{host}] [{response_ms}]".format(**self.log)
        )

    def process_exception(self, request, exception):
        """
        Get the exception info now, in case another exception is thrown later.
        Logs exception as an error when a request cannot return a response.
        400 errors return a response whether either defined by the user or as a default HTTP404 response by Django
        """

        self.log["status_code"] = 500  # Unable to get status code from exception server return 500 as default
        self.log["response_ms"] = self.get_response_ms()
        self.log["status"] = "ERROR"
        self.log["timestamp"] = now().strftime("%d/%m/%y %H:%M:%S")
        self.log["traceback"] = traceback.format_exc()

        self.server_logger.error("%s", self.get_message_string(), extra=self.log)

    def get_response_ms(self):
        """Returns time elapsed from request to response/exception"""
        duration = perf_counter() - self.start
        return int(duration * 1000)
