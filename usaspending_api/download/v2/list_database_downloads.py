from django.conf import settings
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.download.helpers.latest_download_file_helpers import (
    get_last_modified_download_file_by_prefix,
    remove_file_prefix_if_exists,
)
from usaspending_api.common.cache_decorator import cache_response


class ListDatabaseDownloadsViewSet(APIView):
    """
    Returns information about the latest full and subset database download files.
    """

    redirect_dir = settings.DATABASE_DOWNLOAD_S3_REDIRECT_DIR

    @cache_response()
    def get(self, request):

        full_download_prefix = f"{self.redirect_dir}/usaspending-db_"
        latest_full_download_name = get_last_modified_download_file_by_prefix(full_download_prefix)
        latest_full_download_name = remove_file_prefix_if_exists(latest_full_download_name, f"{self.redirect_dir}/")

        subset_download_prefix = f"{self.redirect_dir}/usaspending-db-subset_"
        latest_subset_download_name = get_last_modified_download_file_by_prefix(subset_download_prefix)
        latest_subset_download_name = remove_file_prefix_if_exists(latest_subset_download_name, f"{self.redirect_dir}/")

        results = {
            "full_download_file": self._structure_file_response(latest_full_download_name),
            "subset_download_file": self._structure_file_response(latest_subset_download_name),
            "messages": [],
        }
        return Response(results)

    def _structure_file_response(self, file_name):
        return {
            "file_name": file_name,
            "url": (
                f"{settings.FILES_SERVER_BASE_URL}/{self.redirect_dir}/{file_name}" if file_name is not None else None
            ),
        }