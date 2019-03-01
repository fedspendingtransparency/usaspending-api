from datetime import datetime, timezone
from usaspending_api.references.models import ToptierAgency
from usaspending_api.download.lookups import VALUE_MAPPINGS


def create_unique_filename(download_types, request_agency=None):
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
