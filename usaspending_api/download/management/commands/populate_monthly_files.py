import logging
import datetime
import json
import boto3
import re

from django.conf import settings
from django.core.management.base import BaseCommand
from usaspending_api.awards.v2.lookups.lookups import procurement_type_mapping, assistance_type_mapping
from usaspending_api.common.helpers.dict_helpers import order_nested_object
from usaspending_api.common.helpers.fiscal_year_helpers import generate_fiscal_year
from usaspending_api.common.helpers.s3_helpers import multipart_upload
from usaspending_api.common.sqs.sqs_handler import get_sqs_queue
from usaspending_api.download.filestreaming import download_generation
from usaspending_api.download.helpers import pull_modified_agencies_cgacs
from usaspending_api.download.lookups import JOB_STATUS_DICT
from usaspending_api.download.models.download_job import DownloadJob
from usaspending_api.download.v2.request_validations import AwardDownloadValidator
from usaspending_api.references.models import ToptierAgency

logger = logging.getLogger(__name__)

award_mappings = {
    "contracts": list(procurement_type_mapping.keys()),
    "assistance": list(assistance_type_mapping.keys()),
}


class Command(BaseCommand):
    def download(
        self,
        file_name,
        prime_award_types=None,
        agency=None,
        sub_agency=None,
        date_type=None,
        start_date=None,
        end_date=None,
        columns=[],
        file_format="csv",
        monthly_download=False,
        cleanup=False,
        use_sqs=False,
    ):
        date_range = {}
        if start_date:
            date_range["start_date"] = start_date
        if end_date:
            date_range["end_date"] = end_date
        json_request = {
            "constraint_type": "year",
            "filters": {
                "prime_award_types": prime_award_types,
                "agency": str(agency),
                "date_type": date_type,
                "date_range": date_range,
            },
            "columns": columns,
            "file_format": file_format,
        }
        award_download = AwardDownloadValidator(json_request)
        validated_request = award_download.json_request
        download_job = DownloadJob.objects.create(
            job_status_id=JOB_STATUS_DICT["ready"],
            file_name=file_name,
            json_request=json.dumps(order_nested_object(validated_request)),
            monthly_download=True,
        )

        if not use_sqs:
            # Note: Because of the line below, it's advised to only run this script on a separate instance as this will
            #       modify your bulk download settings.
            settings.BULK_DOWNLOAD_S3_BUCKET_NAME = settings.MONTHLY_DOWNLOAD_S3_BUCKET_NAME
            download_generation.generate_download(download_job=download_job)
            if cleanup:
                # Get all the files that have the same prefix except for the update date
                file_name_prefix = file_name[:-12]  # subtracting the 'YYYYMMDD.zip'
                for key in self.bucket.objects.filter(Prefix=file_name_prefix):
                    if key.key == file_name:
                        # ignore the one we just uploaded
                        continue
                    key.delete()
                    logger.info("Deleting {} from bucket".format(key.key))
        else:
            queue = get_sqs_queue(queue_name=settings.BULK_DOWNLOAD_SQS_QUEUE_NAME)
            queue.send_message(MessageBody=str(download_job.download_job_id))

    def upload_placeholder(self, file_name, empty_file):
        bucket = settings.BULK_DOWNLOAD_S3_BUCKET_NAME
        region = settings.USASPENDING_AWS_REGION

        logger.info("Uploading {}".format(file_name))
        multipart_upload(bucket, region, empty_file, file_name)

    def add_arguments(self, parser):
        parser.add_argument(
            "--local",
            action="store_true",
            dest="local",
            default=False,
            help="Generate all the files locally. Note they will still be uploaded to the S3.",
        )
        parser.add_argument(
            "--clobber",
            action="store_true",
            dest="clobber",
            default=False,
            help="Uploads files regardless if they have already been uploaded that day.",
        )
        parser.add_argument(
            "--use_modified_list",
            action="store_true",
            dest="use_modified_list",
            default=False,
            help="Uses the modified agency list instead of the standard agency list",
        )
        parser.add_argument(
            "--agencies",
            dest="agencies",
            nargs="+",
            default=None,
            type=str,
            help="Specific toptier agency database ids (overrides use_modified_list)."
            " Note 'all' may be provided to account for the downloads that comprise "
            " all agencies for a fiscal_year.",
        )
        parser.add_argument(
            "--award_types",
            dest="award_types",
            nargs="+",
            default=["assistance", "contracts"],
            type=str,
            help="Specific award types, must be 'contracts' and/or 'assistance'",
        )
        parser.add_argument(
            "--fiscal_years", dest="fiscal_years", nargs="+", default=None, type=int, help="Specific Fiscal Years"
        )
        parser.add_argument(
            "--placeholders",
            action="store_true",
            dest="placeholders",
            default=False,
            help="Upload empty files as placeholders.",
        )
        parser.add_argument(
            "--cleanup",
            action="store_true",
            dest="cleanup",
            default=False,
            help="Deletes the previous version of the newly generated file after uploading"
            " (only applies if --local is also provided).",
        )
        parser.add_argument(
            "--empty-assistance-file",
            dest="empty_assistance_file",
            default="",
            help="Empty assistance file for uploading",
        )
        parser.add_argument(
            "--empty-contracts-file", dest="empty_contracts_file", default="", help="Empty contracts file for uploading"
        )

    def handle(self, *args, **options):
        """Run the application."""

        # Make sure
        #   settings.BULK_DOWNLOAD_S3_BUCKET_NAME
        #   settings.BULK_DOWNLOAD_SQS_QUEUE_NAME
        #   settings.USASPENDING_AWS_REGION
        # are properly configured!

        local = options["local"]
        clobber = options["clobber"]
        use_modified_list = options["use_modified_list"]
        agencies = options["agencies"]
        award_types = options["award_types"]
        for award_type in award_types:
            if award_type not in ["contracts", "assistance"]:
                raise Exception("Unacceptable award type: {}".format(award_type))
        fiscal_years = options["fiscal_years"]
        placeholders = options["placeholders"]
        cleanup = options["cleanup"]
        empty_assistance_file = options["empty_assistance_file"]
        empty_contracts_file = options["empty_contracts_file"]
        if placeholders and (not empty_assistance_file or not empty_contracts_file):
            raise Exception("Placeholder arg provided but empty files not provided")

        current_date = datetime.date.today()
        updated_date_timestamp = datetime.datetime.strftime(current_date, "%Y%m%d")

        toptier_agencies = ToptierAgency.objects.all()
        include_all = True
        if use_modified_list:
            used_cgacs = set(pull_modified_agencies_cgacs())
            toptier_agencies = ToptierAgency.objects.filter(toptier_code__in=used_cgacs)
        if agencies:
            if "all" in agencies:
                agencies.remove("all")
            else:
                include_all = False
            toptier_agencies = ToptierAgency.objects.filter(toptier_agency_id__in=agencies)
        toptier_agencies = list(toptier_agencies.values("name", "toptier_agency_id", "toptier_code"))
        # Adding 'all' to prevent duplication of code
        if include_all:
            toptier_agencies.append({"name": "All", "toptier_agency_id": "all", "toptier_code": "All"})
        if not fiscal_years:
            fiscal_years = range(2001, generate_fiscal_year(current_date) + 1)

        # moving it to self.bucket as it may be used in different cases
        bucket_name = settings.MONTHLY_DOWNLOAD_S3_BUCKET_NAME
        region_name = settings.USASPENDING_AWS_REGION
        self.bucket = boto3.resource("s3", region_name=region_name).Bucket(bucket_name)

        if not clobber:
            reuploads = []
            for key in self.bucket.objects.all():
                re_match = re.findall("(.*)_Full_{}.zip".format(updated_date_timestamp), key.key)
                if re_match:
                    reuploads.append(re_match[0])

        logger.info("Generating {} files...".format(len(toptier_agencies) * len(fiscal_years) * 2))
        for agency in toptier_agencies:
            for fiscal_year in fiscal_years:
                start_date = "{}-10-01".format(fiscal_year - 1)
                end_date = "{}-09-30".format(fiscal_year)
                for award_type in award_types:
                    file_name = f"FY{fiscal_year}_{agency['toptier_code']}_{award_type.capitalize()}"
                    full_file_name = f"{file_name}_Full_{updated_date_timestamp}.zip"
                    if not clobber and file_name in reuploads:
                        logger.info(f"Skipping already uploaded: {full_file_name}")
                        continue
                    if placeholders:
                        empty_file = empty_contracts_file if award_type == "contracts" else empty_assistance_file
                        self.upload_placeholder(file_name=full_file_name, empty_file=empty_file)
                    else:
                        self.download(
                            file_name=full_file_name,
                            prime_award_types=award_mappings[award_type],
                            agency=agency["toptier_agency_id"],
                            date_type="action_date",
                            start_date=start_date,
                            end_date=end_date,
                            monthly_download=True,
                            cleanup=cleanup,
                            use_sqs=(not local),
                        )
        logger.info("Populate Monthly Files complete")
