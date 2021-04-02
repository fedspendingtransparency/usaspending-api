import logging
import os
import csv
from datetime import datetime
from dateutil.relativedelta import relativedelta

from django.conf import settings
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.elasticsearch.filter_helpers import create_fiscal_year_filter

logger = logging.getLogger(__name__)

# Based on SAM Functional Data Dictionary
SAM_FUNCTIONAL_DATA_DICTIONARY_CSV = str(settings.APP_DIR / "data" / "sam_functional_data_dictionary.csv")
DUNS_BUSINESS_TYPES_MAPPING = {}


def validate_year(year=None):
    if year and not (year.isdigit() or year in ["all", "latest"]):
        raise InvalidParameterException("Invalid year: {}.".format(year))
    return year


def reshape_filters(duns_search_texts=[], recipient_id=None, state_code=None, year=None, award_type_codes=None):
    # recreate filters for spending over time/category
    filters = {}

    if duns_search_texts:
        filters["recipient_search_text"] = duns_search_texts

    if recipient_id:
        filters["recipient_id"] = recipient_id

    if state_code:
        filters["place_of_performance_locations"] = [{"country": "USA", "state": state_code}]

    if year:
        today = datetime.now()
        if year and year.isdigit():
            time_period = create_fiscal_year_filter(year)
        elif year == "all":
            time_period = [
                {"start_date": settings.API_SEARCH_MIN_DATE, "end_date": datetime.strftime(today, "%Y-%m-%d")}
            ]
        else:
            last_year = today - relativedelta(years=1)
            time_period = [
                {
                    "start_date": datetime.strftime(last_year, "%Y-%m-%d"),
                    "end_date": datetime.strftime(today, "%Y-%m-%d"),
                }
            ]
        filters["time_period"] = time_period

    if award_type_codes:
        filters["award_type_codes"] = award_type_codes

    return filters


def get_duns_business_types_mapping():
    if not DUNS_BUSINESS_TYPES_MAPPING:
        if not os.path.exists(SAM_FUNCTIONAL_DATA_DICTIONARY_CSV):
            logger.warning("SAM Functional Data Dictionary CSV not found. DUNS business types not loaded.")
            return {}
        with open(SAM_FUNCTIONAL_DATA_DICTIONARY_CSV, "r") as sam_data_dict_csv:
            reader = csv.DictReader(sam_data_dict_csv, delimiter=",")
            rows = list(reader)
            for index, row in enumerate(rows, 1):
                DUNS_BUSINESS_TYPES_MAPPING[row["code"]] = row["terse_label"]
    return DUNS_BUSINESS_TYPES_MAPPING
