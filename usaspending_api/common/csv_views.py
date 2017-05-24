from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status
from rest_framework.utils.urls import replace_query_param

from django.conf import settings

from usaspending_api.common.models import RequestCatalog
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.csv_helpers import format_path, s3_get_url, sqs_add_to_queue, resolve_path_to_view

import logging


class CsvDownloadView(APIView):
    exception_logger = logging.getLogger("exceptions")

    def process_csv_download(self, path, request):
        try:
            created, self.req = RequestCatalog.get_or_create_from_request(request)
            response = {
                "request_checksum": self.req.checksum,
                "request_path": format_path(path),
                "status": "",
                "retry_url": replace_query_param(request.build_absolute_uri().split("?")[0], "req", self.req.checksum),
                "location": None
            }

            location = None
            try:
                # Get URL location from S3
                location = s3_get_url(path, self.req.checksum)

                if not location:
                    try:
                        # Make sure the View they want is supported
                        view = None
                        try:
                            view = resolve_path_to_view(format_path(path))
                        except:
                            # It's fine to catch-all exceptions here because we only care about the valid case
                            view = None

                        if not view:
                            response["status"] = "Requested path is not currently supported by CSV bulk download"
                            status_code = status.HTTP_400_BAD_REQUEST
                        else:
                            sqs_add_to_queue(path, self.req.checksum)
                            response["status"] = "File has been queued for generation."
                            status_code = status.HTTP_202_ACCEPTED
                    except Exception as e:
                        # We couldn't connect to SQS
                        response["status"] = "Error queueing file: {}".format(str(e))
                        status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
                else:
                    response["status"] = "File is ready for download."
                    response["location"] = location
                    status_code = status.HTTP_200_OK
            except Exception as e:
                # We couldn't connect to S3
                response["status"] = "Error finding file: {}".format(str(e))
                status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
        except InvalidParameterException as e:
            response = {"message": str(e)}
            status_code = status.HTTP_400_BAD_REQUEST
            if 'req' in self.__dict__:
                # If we've made a request catalog, but the request is bad, we need to delete it
                self.req.delete()
            self.exception_logger.exception(e)
        except Exception as e:
            response = {"message": str(e)}
            status_code = status.HTTP_500_INTERNAL_SERVER_ERROR
            if 'req' in self.__dict__:
                # If we've made a request catalog, but the request is bad, we need to delete it
                self.req.delete()
            self.exception_logger.exception(e)
        finally:
            return Response(response, status=status_code)

    def get(self, request, path, *args, **kwargs):
        return self.process_csv_download(path, request)

    def post(self, request, path, *args, **kwargs):
        return self.process_csv_download(path, request)
