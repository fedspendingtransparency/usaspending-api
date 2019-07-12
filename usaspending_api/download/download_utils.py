from datetime import datetime, timezone
from usaspending_api.common.helpers.text_helpers import slugify_text_for_file_names
from usaspending_api.common.logging import get_remote_addr
from usaspending_api.download.helpers import write_to_download_log
from usaspending_api.download.lookups import VALUE_MAPPINGS
from usaspending_api.references.models import ToptierAgency


def create_unique_filename(json_request, request_agency=None):
    if json_request.get("is_for_idv"):
        download_name = "IDV_" + slugify_text_for_file_names(json_request.get("piid"), "UNKNOWN", 50)
    else:
        download_types = json_request["download_types"]
        prefix = obtain_filename_prefix_from_agency_id(request_agency)
        award_type_name = create_award_level_string(download_types)
        download_name = "{}_{}".format(prefix, award_type_name)
    timestamped_file_name = get_timestamped_filename("{}.zip".format(download_name))
    return timestamped_file_name


def obtain_filename_prefix_from_agency_id(request_agency):
    result = "all"
    if request_agency:
        toptier_agency_filter = ToptierAgency.objects.filter(toptier_agency_id=request_agency).first()
        if toptier_agency_filter:
            result = toptier_agency_filter.cgac_code
    return result


def create_award_level_string(download_types):
    return "_".join(VALUE_MAPPINGS[award_level]["download_name"] for award_level in download_types)


def get_timestamped_filename(filename, datetime_format="%Y%m%d%H%M%S%f"):
    """
        Gets a Timestamped file name to prevent conflicts on S3 Uploading
    """
    file_sans_extension, file_extension = filename.split(".")
    timestamp = datetime.strftime(datetime.now(timezone.utc), datetime_format)
    return "{}_{}.{}".format(file_sans_extension, timestamp, file_extension)


def log_new_download_job(request, download_job):
    write_to_download_log(
        message='Starting new download job [{}]'.format(download_job.download_job_id),
        download_job=download_job,
        other_params={'request_addr': get_remote_addr(request)}
    )
