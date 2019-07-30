import datetime


TAS_COMPONENT_TO_FIELD_MAPPING = {
    "ata": "allocation_transfer_agency_id",
    "aid": "agency_id",
    "bpoa": "beginning_period_of_availability",
    "epoa": "ending_period_of_availability",
    "a": "availability_type_code",
    "main": "main_account_code",
    "sub": "sub_account_code",
}


def start_and_end_dates_from_fyq(fiscal_year, fiscal_quarter):
    if fiscal_quarter == 1:
        start_date = datetime.date(fiscal_year - 1, 10, 1)
        end_date = datetime.date(fiscal_year - 1, 12, 31)
    elif fiscal_quarter == 2:
        start_date = datetime.date(fiscal_year, 1, 1)
        end_date = datetime.date(fiscal_year, 3, 31)
    elif fiscal_quarter == 3:
        start_date = datetime.date(fiscal_year, 4, 1)
        end_date = datetime.date(fiscal_year, 6, 30)
    else:
        start_date = datetime.date(fiscal_year, 7, 1)
        end_date = datetime.date(fiscal_year, 9, 30)

    return start_date, end_date
