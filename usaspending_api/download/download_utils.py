from datetime import datetime, timezone

from django.conf import settings

from usaspending_api.common.helpers.text_helpers import slugify_text_for_file_names
from usaspending_api.common.logging import get_remote_addr
from usaspending_api.download.helpers import write_to_download_log
from usaspending_api.download.lookups import VALUE_MAPPINGS
from usaspending_api.references.models import ToptierAgency


def create_unique_filename(json_request, origination=None):
    timestamp = datetime.strftime(datetime.now(timezone.utc), "%Y-%m-%d_H%HM%MS%S%f")
    request_agency = json_request.get("agency", "all")

    if json_request.get("is_for_idv"):
        download_name = f"IDV_{slugify_text_for_file_names(json_request.get('piid'), 'UNKNOWN', 50)}_{timestamp}.zip"
    elif json_request.get("is_for_contract"):
        download_name = f"CONT_{slugify_text_for_file_names(json_request.get('piid'), 'UNKNOWN', 50)}_{timestamp}.zip"
    elif json_request.get("is_for_assistance"):
        slug_text = slugify_text_for_file_names(json_request.get("assistance_id"), "UNKNOWN", 50)
        download_name = f"ASST_{slug_text}_{timestamp}.zip"
    elif json_request["request_type"] == "disaster":
        download_name = f"{settings.COVID19_DOWNLOAD_FILENAME_PREFIX}_{timestamp}.zip"
    elif json_request["request_type"] == "disaster_recipient":
        download_name = f"COVID-19_Recipients_{json_request['award_category']}_{timestamp}.zip"
    elif json_request["request_type"] == "account":
        file_name_template = obtain_zip_filename_format(json_request["download_types"])
        agency = obtain_filename_prefix_from_agency_id(request_agency)
        level = "FA" if json_request["account_level"] == "federal_account" else "TAS"
        data_quarters = construct_data_date_range(json_request["filters"])

        download_name = file_name_template.format(
            agency=agency,
            data_quarters=data_quarters,
            level=level,
            timestamp=timestamp,
        )
    else:  # "award" downloads
        agency = ""

        # Since Keyword Search using the "Bulk Download" Endpoint for unknown reasons
        # Check for the specific filter to mimic the Advanced Search download filename
        if origination == "bulk_download" and "transaction_keyword_search" not in json_request["filters"]:
            agency = obtain_filename_prefix_from_agency_id(request_agency) + "_"

        award_type_name = create_award_level_string(json_request["download_types"])
        download_name = f"{agency}{award_type_name}_{timestamp}.zip"

    return download_name


def obtain_zip_filename_format(download_types):
    if len(download_types) > 1:
        return "{data_quarters}_{agency}_{level}_AccountData_{timestamp}.zip"
    return f"{VALUE_MAPPINGS[download_types[0]]['zipfile_template']}.zip"


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
    return "And".join(type_list)


def log_new_download_job(request, download_job):
    write_to_download_log(
        message="Starting new download job [{}]".format(download_job.download_job_id),
        download_job=download_job,
        other_params={"request_addr": get_remote_addr(request)},
    )


def construct_data_date_range(provided_filters: dict) -> str:
    if provided_filters.get("fy") is not None and provided_filters.get("fy") > 2019:
        if provided_filters.get("fy") == 2020:
            string = f"FY{provided_filters.get('fy')}Q1"
        else:
            string = f"FY{provided_filters.get('fy')}P01"
        if provided_filters.get("period") is not None and provided_filters.get("period") != 1:
            string += f"-P{provided_filters.get('period'):0>2}"
        elif provided_filters.get("period") is None:
            string += f"-Q{provided_filters.get('quarter')}"
    else:
        string = f"FY{provided_filters.get('fy')}Q1"
        if provided_filters.get("quarter") != 1:
            string += f"-Q{provided_filters.get('quarter')}"
    return string
