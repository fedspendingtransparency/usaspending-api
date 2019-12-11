from datetime import datetime, timezone
from usaspending_api.common.helpers.text_helpers import slugify_text_for_file_names
from usaspending_api.common.logging import get_remote_addr
from usaspending_api.download.helpers import write_to_download_log
from usaspending_api.download.lookups import VALUE_MAPPINGS
from usaspending_api.references.models import ToptierAgency


def create_unique_filename(json_request, request_agency=None):
    if json_request.get("is_for_idv"):
        download_name = "IDV_" + slugify_text_for_file_names(json_request.get("piid"), "UNKNOWN", 50)
    elif json_request.get("is_for_contract"):
        download_name = "CONT_" + slugify_text_for_file_names(json_request.get("piid"), "UNKNOWN", 50)
    elif json_request.get("is_for_assistance"):
        download_name = "ASST_" + slugify_text_for_file_names(json_request.get("assistance_id"), "UNKNOWN", 50)
    elif json_request["request_type"] == "account":
        file_name_template = obtain_zip_filename_format(json_request["download_types"])
        prefix = obtain_filename_prefix_from_agency_id(request_agency)
        level = "FA" if json_request["account_level"] == "federal_account" else "TAS"
        additional_quarters = ""

        if json_request["filters"]["quarter"] != 1:
            additional_quarters = f"-Q{json_request['filters']['quarter']}"

        download_name = file_name_template.format(
            fy=json_request["filters"]["fy"], date_range=additional_quarters, level=level, agency=prefix
        )
    else:
        if json_request.get("constraint_type", "") == "year":
            prefix = "All_" if request_agency == "all" else f"{request_agency}_"
        else:
            prefix = ""
        download_types = json_request["download_types"]
        agency = obtain_filename_prefix_from_agency_id(request_agency)
        award_type_name = create_award_level_string(download_types)
        download_name = f"{prefix}{agency}_{award_type_name}"

    datetime_format = "%Y-%m-%d_H%HM%MS%S%f"
    timestamped_file_name = get_timestamped_filename(f"{download_name}.zip", datetime_format=datetime_format)
    return timestamped_file_name


def obtain_zip_filename_format(download_types):
    if len(download_types) > 1:
        raise NotImplementedError
    return VALUE_MAPPINGS[download_types[0]]["zipfile_template"]


def obtain_filename_prefix_from_agency_id(request_agency):
    result = "All"
    if request_agency and request_agency != "all":
        toptier_agency_filter = ToptierAgency.objects.filter(toptier_agency_id=request_agency).first()
        if toptier_agency_filter:
            result = toptier_agency_filter.toptier_code
    return result


def create_award_level_string(download_types):
    type_list = []
    for award_level in download_types:
        if "type_name" in VALUE_MAPPINGS[award_level]:
            type_list.append(VALUE_MAPPINGS[award_level]["type_name"])
        else:
            type_list.append(VALUE_MAPPINGS[award_level]["download_name"])
    return "+".join(type_list)


def get_timestamped_filename(filename, datetime_format="%Y%m%d%H%M%S%f"):
    """Return an updated filename to include current timestamp"""
    file_sans_extension, file_extension = filename.split(".")
    timestamp = datetime.strftime(datetime.now(timezone.utc), datetime_format)
    return "{}_{}.{}".format(file_sans_extension, timestamp, file_extension)


def log_new_download_job(request, download_job):
    write_to_download_log(
        message="Starting new download job [{}]".format(download_job.download_job_id),
        download_job=download_job,
        other_params={"request_addr": get_remote_addr(request)},
    )
