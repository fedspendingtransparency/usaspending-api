from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status

from django.conf import settings

from usaspending_api.common.models import RequestCatalog
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.csv_helpers import format_path, s3_get_url, sqs_add_to_queue, create_filename_from_options

import logging


class CSVdownloadView(APIView):
    exception_logger = logging.getLogger("exceptions")

    def process_csv_download(self, path, request):
        try:
            created, self.req = RequestCatalog.get_or_create_from_request(request)

            response = {
                "request_checksum": self.req.checksum,
                "request_path": format_path(request_path),
                "status": "",
                "location": None
            }

            location = None
            try:
                # Get URL location from S3
                location = s3_get_url(path, self.req.checksum)

                if not location:
                    try:
                        sqs_add_to_queue(path, self.req.checksum)
                        response["status"] = "File has been queued for generation."
                        status_code = status.HTTP_200_OK
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
