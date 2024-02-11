import boto3
import re

from django.conf import settings
from rest_framework.response import Response
from rest_framework.views import APIView

from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.download.filestreaming.s3_handler import S3Handler
from usaspending_api.references.models import ToptierAgency


class ListUnlinkedAwardsDownloadsViewSet(APIView):
    """
    Returns a list which contains links to the latest versions of unlinked awards files for an agency.
    """

    endpoint_doc = "usaspending_api/api_contracts/contracts/v2/bulk_download/list_unlinked_awards_files.md"

    s3_handler = S3Handler(
        bucket_name=settings.BULK_DOWNLOAD_S3_BUCKET_NAME,
        redirect_dir=settings.UNLINKED_AWARDS_DOWNLOAD_REDIRECT_DIR,
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
        if not agency_check:
            raise InvalidParameterException(f"Agency not found given the toptier code: {toptier_code}")
        agency = agency_check[0]

        # Populate regex
        monthly_download_prefixes = f"FY{fiscal_year}_{agency['toptier_code']}_{download_type}"
        monthly_download_regex = r"{}_Full_.*\.zip".format(monthly_download_prefixes)
        delta_download_prefixes = f"FY(All)_{agency['toptier_code']}_{download_type}"
        delta_download_regex = r"FY\(All\)_{}_{}_Delta_.*\.zip".format(agency["toptier_code"], download_type)

        # Retrieve and filter the files we need
        bucket = boto3.resource("s3", region_name=self.s3_handler.region).Bucket(self.s3_handler.bucketRoute)
        monthly_download_names = list(
            filter(
                re.compile(monthly_download_regex).search,
                [key.key for key in bucket.objects.filter(Prefix=monthly_download_prefixes)],
            )
        )
        delta_download_names = list(
            filter(
                re.compile(delta_download_regex).search,
                [key.key for key in bucket.objects.filter(Prefix=delta_download_prefixes)],
            )
        )

        # Generate response
        downloads = []
        for filename in monthly_download_names:
            downloads.append(self.create_download_response_obj(filename, fiscal_year, type_param, agency))
        for filename in delta_download_names:
            downloads.append(self.create_download_response_obj(filename, None, type_param, agency, is_delta=True))

        return Response({"monthly_files": downloads})

    def create_download_response_obj(self, filename, fiscal_year, type_param, agency, is_delta=False):
        """Return a"""
        regex = r"(.*)_(.*)_Delta_(.*)\.zip" if is_delta else r"(.*)_(.*)_(.*)_Full_(.*)\.zip"
        filename_data = re.findall(regex, filename)[0]

        # Simply adds dashes for the date, 20180101 -> 2018-01-01, could also use strftime
        unformatted_date = filename_data[2 if is_delta else 3]
        updated_date = "-".join([unformatted_date[:4], unformatted_date[4:6], unformatted_date[6:]])

        return {
            "fiscal_year": fiscal_year,
            "agency_name": agency["name"],
            "agency_acronym": agency["abbreviation"],
            "type": type_param,
            "updated_date": updated_date,
            "file_name": filename,
            "url": self.s3_handler.get_simple_url(file_name=filename),
        }
