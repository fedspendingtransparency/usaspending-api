from django.conf import settings
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.download.helpers.download_file_helpers import (
    get_last_modified_download_file,
)
from usaspending_api.common.cache_decorator import cache_response


class ListDatabaseDownloadsViewSet(APIView):
    """
    Returns information about the latest full and subset database download files.
    """

    redirect_dir = settings.DATABASE_DOWNLOAD_S3_REDIRECT_DIR

    @cache_response()
    def get(self, request):

        full_download_prefix = "usaspending-db_"
        latest_full_download_name = get_last_modified_download_file(
            full_download_prefix, settings.DATABASE_DOWNLOAD_S3_BUCKET_NAME
        )

        subset_download_prefix = "usaspending-db-subset_"
        latest_subset_download_name = get_last_modified_download_file(
            subset_download_prefix, settings.DATABASE_DOWNLOAD_S3_BUCKET_NAME
        )

        results = {
            "full_download_file": self._structure_file_response(latest_full_download_name),
            "subset_download_file": self._structure_file_response(latest_subset_download_name),
            "messages": [],
        }
        return Response(results)

    def _structure_file_response(self, file_name: str):
        """Accepts a file_name and builds a reponse dictionary containing a file_name and a constructed
        url based on application settings. If no file_name is provided, url will be None
        Args:
            file_name: Name of file used to construct dictionary.
        Returns:
            Dictionary containing file_name and url information.
        """
        return {
            "file_name": file_name,
            "url": (
                f"{settings.FILES_SERVER_BASE_URL}/{self.redirect_dir}/{file_name}" if file_name is not None else None
            ),
        }
