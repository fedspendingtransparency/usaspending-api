from django.conf import settings
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.common.cache_decorator import cache_response
from usaspending_api.download.helpers.download_file_helpers import (
    get_last_modified_download_file,
    remove_file_prefix_if_exists,
)
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.references.models import ToptierAgency


class ListUnlinkedAwardsDownloadsViewSet(APIView):
    """
    Returns a list which contains links to the latest versions of unlinked awards files for an agency.
    """

    redirect_dir = settings.UNLINKED_AWARDS_DOWNLOAD_REDIRECT_DIR

    @cache_response()
    def post(self, request):
        """Return list of downloads that match the requested params."""
        toptier_code = request.data.get("toptier_code", None)

        # Check required params
        required_params = {"toptier_code": toptier_code}
        for required, param_value in required_params.items():
            if param_value is None:
                raise InvalidParameterException(f"Missing one or more required body parameters: {required}")

        agency_check = ToptierAgency.objects.filter(toptier_code=toptier_code).values(
            "toptier_code", "name", "abbreviation"
        )
        if not agency_check or len(agency_check.all()) == 0:
            raise InvalidParameterException(f"Agency not found given the toptier code: {toptier_code}")
        agency = agency_check[0]

        # Populate regex
        agency_name = agency["name"]
        for char in settings.UNLINKED_AWARDS_AGENCY_NAME_CHARS_TO_REPLACE:
            agency_name = agency_name.replace(char, "_")

        download_prefix = f"{self.redirect_dir}/{agency_name}_UnlinkedAwards_"
        latest_download_name = get_last_modified_download_file(download_prefix, settings.BULK_DOWNLOAD_S3_BUCKET_NAME)
        latest_download_file_name = remove_file_prefix_if_exists(latest_download_name, f"{self.redirect_dir}/")

        identified_download = {
            "agency_name": agency["name"],
            "toptier_code": agency["toptier_code"],
            "agency_acronym": agency["abbreviation"],
            "file_name": latest_download_file_name,
            "url": (
                f"{settings.FILES_SERVER_BASE_URL}/{self.redirect_dir}/{latest_download_file_name}"
                if latest_download_file_name is not None
                else None
            ),
        }

        results = {"file": identified_download, "messages": []}
        return Response(results)
