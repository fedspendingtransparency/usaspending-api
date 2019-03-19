import logging
import pandas as pd

from django.core.management.base import BaseCommand

from usaspending_api.common.url_helpers import FileFromUrl, DISPLAY_ALL_SCHEMAS
from usaspending_api.references.models import Cfda

logger = logging.getLogger("console")

DATA_CLEANING_MAP = {
    "program_title": "program_title",
    "program_number": "program_number",
    "popular_name_(020)": "popular_name",
    "federal_agency_(030)": "federal_agency",
    "authorization_(040)": "authorization",
    "objectives_(050)": "objectives",
    "types_of_assistance_(060)": "types_of_assistance",
    "uses_and_use_restrictions_(070)": "uses_and_use_restrictions",
    "applicant_eligibility_(081)": "applicant_eligibility",
    "beneficiary_eligibility_(082)": "beneficiary_eligibility",
    "credentials/documentation_(083)": "credentials_documentation",
    "preapplication_coordination_(091)": "pre_application_coordination",
    "application_procedures_(092)": "application_procedures",
    "award_procedure_(093)": "award_procedure",
    "deadlines_(094)": "deadlines",
    "range_of_approval/disapproval_time_(095)": "range_of_approval_disapproval_time",
    "appeals_(096)": "appeals",
    "renewals_(097)": "renewals",
    "formula_and_matching_requirements_(101)": "formula_and_matching_requirements",
    "length_and_time_phasing_of_assistance_(102)": "length_and_time_phasing_of_assistance",
    "reports_(111)": "reports",
    "audits_(112)": "audits",
    "records_(113)": "records",
    "account_identification_(121)": "account_identification",
    "obligations_(122)": "obligations",
    "range_and_average_of_financial_assistance_(123)": "range_and_average_of_financial_assistance",
    "program_accomplishments_(130)": "program_accomplishments",
    "regulations__guidelines__and_literature_(140)": "regulations_guidelines_and_literature",
    "regional_or__local_office_(151)": "regional_or_local_office",
    "headquarters_office_(152)": "headquarters_office",
    "website_address_(153)": "website_address",
    "related_programs_(160)": "related_programs",
    "examples_of_funded_projects_(170)": "examples_of_funded_projects",
    "criteria_for_selecting_proposals_(180)": "criteria_for_selecting_proposals",
    "url": "url",
    "recovery": "recovery",
    "omb_agency_code": "omb_agency_code",
    "omb_bureau_code": "omb_bureau_code",
    "published_date": "published_date",
    "archived_date": "archived_date",
}


def clean_col_names(field):
    """Define some data-munging functions that can be applied to pandas
    dataframes as necessary"""
    return str(field).lower().strip().replace(" ", "_").replace(",", "_")


def insert_dataframe(df, table, engine):
    """Inserts a dataframe to the specified database table."""
    df.to_sql(table, engine, index=False, if_exists="append")
    return len(df.index)


class Command(BaseCommand):

    help = ""

    def add_arguments(self, parser):
        parser.add_argument(
            "cfda-data-url", type=str, help="Provide a valid RFC URL for a CFDA file: {}".format(DISPLAY_ALL_SCHEMAS)
        )

    def handle(self, *args, **options):
        file_opener = FileFromUrl(options["cfda-data-url"])
        with file_opener.fetch_data_from_source(return_file_handle=True) as data_stream:
            regenerated_df = load_cfda_csv_into_pandas(data_stream)

        database_df = load_cfda_table_into_pandas()

        regenerated_df = fully_order_pandas_dataframe(regenerated_df, "program_number")
        database_df = fully_order_pandas_dataframe(database_df, "program_number")
        if not load_cfda(database_df, regenerated_df):
            raise SystemExit(3)


def load_cfda_csv_into_pandas(data_stream):
    df = pd.read_csv(data_stream, dtype=str, encoding="latin1", na_filter=False)
    df.rename(columns=clean_col_names, inplace=True)

    for field in DATA_CLEANING_MAP.keys():
        if field not in list(df.columns):
            raise ValueError("{} is required for loading table".format(field))

    # toss out any columns from the csv that aren't in the fieldMap parameter
    df = df[list(DATA_CLEANING_MAP.keys())]

    # rename columns as specified in fieldMap
    df = df.rename(columns=DATA_CLEANING_MAP)

    df["data_source"] = "USA"
    return df


def load_cfda_table_into_pandas():
    source_data = list(Cfda.objects.all().values())
    database_records = pd.DataFrame(source_data, dtype=str)
    del database_records["id"]
    del database_records["create_date"]
    del database_records["update_date"]
    return database_records


def fully_order_pandas_dataframe(df, sort_column):
    df.sort_values(sort_column, inplace=True)  # sort the values using the provided column
    df[sorted(df.columns.tolist())]  # order the dataframe columns
    df.reset_index(drop=True, inplace=True)  # reset the indexes to match the new order
    return df


def load_cfda(original_df, new_df):
    if new_df.equals(original_df):
        logger.info("Skipping CFDA load, no new data.")
        return False

    new_record_count = 0
    updated_record_count = 0
    deleted_record_count = 0

    # logger.info("Deleting CFDA data.")
    # for cfda in Cfda.objects.all():
    #     if cfda.program_number not in new_df["program_number"]:
    #         cfda.delete()
    #         deleted_record_count += 1
    # logger.info("Completed removing stale data.")

    logger.info("Inserting new CFDA data.")
    for row in new_df.itertuples():
        record = row._asdict()
        del record["Index"]
        _, created = Cfda.objects.update_or_create(program_number=record["program_number"], defaults=record)
        if created:
            new_record_count += 1
        else:
            updated_record_count += 1
    logger.info("Completed loading new data.")
    logger.info(
        "New: {}, Removed: {}, Updated: {}".format(new_record_count, deleted_record_count, updated_record_count)
    )
    return True
