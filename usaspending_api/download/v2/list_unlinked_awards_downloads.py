import re
from typing import Optional
import boto3

from django.conf import settings
from rest_framework.response import Response
from rest_framework.views import APIView
from usaspending_api.awards.management.commands.generate_unlinked_awards_download import AGENCY_NAME_CHARS_TO_REPLACE

from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.config import CONFIG
from usaspending_api.download.filestreaming.s3_handler import S3Handler
from usaspending_api.references.models import ToptierAgency


class ListUnlinkedAwardsDownloadsViewSet(APIView):
    """
    Returns a list which contains links to the latest versions of unlinked awards files for an agency.
    """

    s3_handler = S3Handler(
        bucket_name=settings.BULK_DOWNLOAD_S3_BUCKET_NAME, redirect_dir=settings.UNLINKED_AWARDS_DOWNLOAD_REDIRECT_DIR
    )

    # This is intentionally not cached so that the latest updates to these files are always returned
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
        for char in AGENCY_NAME_CHARS_TO_REPLACE:
            agency_name = agency_name.replace(char, "_")

        download_prefix = f"{agency_name}_UnlinkedAwards"
        download_regex = r"{}_.*\.zip".format(download_prefix)

        # Retrieve and filter the files we need
        if not CONFIG.USE_AWS:
            boto3_session = boto3.session.Session(
                region_name=CONFIG.AWS_REGION,
                aws_access_key_id=CONFIG.AWS_ACCESS_KEY.get_secret_value(),
                aws_secret_access_key=CONFIG.AWS_SECRET_KEY.get_secret_value(),
            )
            s3_resource = boto3_session.resource(
                service_name="s3", region_name=CONFIG.AWS_REGION, endpoint_url=f"http://{CONFIG.AWS_S3_ENDPOINT}"
            )
        else:
            s3_resource = boto3.resource(
                service_name="s3", region_name=CONFIG.AWS_REGION, endpoint_url=f"https://{CONFIG.AWS_S3_ENDPOINT}"
            )
        s3_bucket = s3_resource.Bucket(settings.BULK_DOWNLOAD_S3_BUCKET_NAME)
        download_names = []
        for key in s3_bucket.objects.filter(Prefix=download_prefix):
            if re.match(download_regex, key.key):
                download_names.append(key)

        # Generate response
        latest_download_name = self._get_last_modified_file(download_names)
        identified_dowload = {
            "agency_name": agency["name"],
            "toptier_code": agency["toptier_code"],
            "agency_acronym": agency["abbreviation"],
            "file_name": latest_download_name if latest_download_name is not None else None,
            "url": (
                self.s3_handler.get_simple_url(file_name=latest_download_name)
                if latest_download_name is not None
                else None
            ),
        }

        results = {"file": identified_dowload, "messages": []}
        return Response(results)

    def _get_last_modified_file(self, download_files) -> Optional[str]:
        """Return the last modified file from the list of download files.
        Args:
            download_files (List[s3.ObjectSummary]): The files to identify the last modified file from.
        Returns:
            The file name of the last modified file.
        """
        # Best effort to identify the latest file by assuming the file with the latest last modified date
        # is the latest file to have been generated
        sorted_files = [obj.key for obj in sorted(download_files, key=self._get_last_modified_int, reverse=True)]

        last_added_file = None
        if len(sorted_files) > 0:
            last_added_file = sorted_files[0]
        return last_added_file

    def _get_last_modified_int(self, file) -> int:
        """Returns a number that can be used to sort files by the last modified date.
        Args:
            file: The file to retrieve the last modified date from.
        Returns:
            int: The number that represents the files last modified date.
        """
        return int(file.last_modified.strftime("%s"))
