from rest_framework.views import APIView
from rest_framework.response import Response
from rest_framework import status

from usaspending_api.common.models import RequestCatalog, CSVdownloadableResponse
from usaspending_api.common.exceptions import InvalidParameterException

import logging


class CSVdownloadView(APIView):
    exception_logger = logging.getLogger("exceptions")

    def process_csv_download(self, path, request):
        try:
            created, self.req = RequestCatalog.get_or_create_from_request(request)
            csv_download, created = CSVdownloadableResponse.get_or_create_from_parameters(path, self.req)

            response = {
                "request_checksum": self.req.checksum,
                "request_path": csv_download.request_path,
                "status": csv_download.status_description,
                "status_code": csv_download.status_code,
                "location": csv_download.download_location
            }
            status_code = status.HTTP_200_OK
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
