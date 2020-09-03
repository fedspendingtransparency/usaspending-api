import logging
import numpy as np
import pandas as pd
import re

from collections import deque
from datetime import datetime
from django.db import connections
from django.utils.functional import cached_property
from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.common.helpers.dict_helpers import upper_case_dict_values
from usaspending_api.common.helpers.etl_helpers import update_c_to_d_linkages
from usaspending_api.etl.broker_etl_helpers import dictfetchall
from usaspending_api.etl.submission_loader_helpers.skipped_tas import update_skipped_tas
from usaspending_api.etl.management.load_base import load_data_into_model
from usaspending_api.etl.submission_loader_helpers.bulk_create_manager import BulkCreateManager
from usaspending_api.etl.submission_loader_helpers.disaster_emergency_fund_codes import get_disaster_emergency_fund
from usaspending_api.etl.submission_loader_helpers.object_class import get_object_class_row
from usaspending_api.etl.submission_loader_helpers.program_activities import get_program_activity
from usaspending_api.etl.submission_loader_helpers.treasury_appropriation_account import (
    bulk_treasury_appropriation_account_tas_lookup,
    get_treasury_appropriation_account_tas_lookup,
)


logger = logging.getLogger("script")


class CertifiedAwardFinancialIterator:
    def __init__(self, submission_attributes, chunk_size):
        self.submission_attributes = submission_attributes
        self.chunk_size = chunk_size

        # For managing state.
        self._last_id = None
        self._current_chunk = None

    def _retrieve_and_prepare_next_chunk(self):
        sql = f"""
            select  c.*
            {CertifiedAwardFinancial.get_from_where(self.submission_attributes.submission_id)}
            {"" if self._last_id is None else f"and certified_award_financial_id > {self._last_id}"}
            order   by c.certified_award_financial_id
            limit   {self.chunk_size}
        """

        award_financial_frame = pd.read_sql(sql, connections["data_broker"])

        if award_financial_frame.size > 0:
            award_financial_frame["object_class"] = award_financial_frame.apply(get_object_class_row, axis=1)
            award_financial_frame["program_activity"] = award_financial_frame.apply(
                get_program_activity, axis=1, submission_attributes=self.submission_attributes
            )
            award_financial_frame = award_financial_frame.replace({np.nan: None})

            self._last_id = award_financial_frame["certified_award_financial_id"].max()
            self._current_chunk = deque(award_financial_frame.to_dict(orient="records"))

        else:
            self._last_id = None
            self._current_chunk = None

    def __next__(self):
        if not self._current_chunk:
            self._retrieve_and_prepare_next_chunk()
            if not self._current_chunk:
                raise StopIteration
        return self._current_chunk.popleft()


class CertifiedAwardFinancial:
    """ Abstract away the messy details of how we retrieve and prepare certified_award_financial rows. """

    def __init__(self, submission_attributes, db_cursor, chunk_size):
        self.submission_attributes = submission_attributes
        self.db_cursor = db_cursor
        self.chunk_size = chunk_size

    @cached_property
    def count(self):
        sql = f"select count(*) {self.get_from_where(self.submission_attributes.submission_id)}"
        self.db_cursor.execute(sql)
        return self.db_cursor.fetchall()[0][0]

    @property
    def tas_ids(self):
        sql = f"""
            select distinct c.tas_id
            {self.get_from_where(self.submission_attributes.submission_id)}
            and c.tas_id is not null
        """
        self.db_cursor.execute(sql)
        return dictfetchall(self.db_cursor)

    @staticmethod
    def get_from_where(submission_id):
        return f"""
            from    certified_award_financial c
                    inner join submission s on s.submission_id = c.submission_id
            where   s.submission_id = {submission_id} and
                    (
                        (
                            c.transaction_obligated_amou is not null and
                            c.transaction_obligated_amou != 0
                        ) or (
                            c.gross_outlay_amount_by_awa_cpe is not null and
                            c.gross_outlay_amount_by_awa_cpe != 0
                        )
                    )
        """

    def __iter__(self):
        return CertifiedAwardFinancialIterator(self.submission_attributes, self.chunk_size)


def get_file_c(submission_attributes, db_cursor, chunk_size):
    return CertifiedAwardFinancial(submission_attributes, db_cursor, chunk_size)


def load_file_c(submission_attributes, db_cursor, certified_award_financial):
    """
    Process and load file C broker data.
    Note: this should run AFTER the D1 and D2 files are loaded because we try to join to those records to retrieve some
    additional information about the awarding sub-tier agency.
    """
    # this matches the file b reverse directive, but am repeating it here to ensure that we don't overwrite it as we
    # change up the order of file loading

    if certified_award_financial.count == 0:
        logger.warning("No File C (award financial) data found, skipping...")
        return

    reverse = re.compile(r"(_(cpe|fyb)$)|^transaction_obligated_amount$")

    # dictionary to capture TAS that were skipped and some metadata
    # tas = top-level key
    # count = number of rows skipped
    # rows = row numbers skipped, corresponding to the original row numbers in the file that was submitted
    skipped_tas = {}
    total_rows = certified_award_financial.count
    start_time = datetime.now()

    bulk_treasury_appropriation_account_tas_lookup(certified_award_financial.tas_ids, db_cursor)

    _save_file_c_rows(certified_award_financial, total_rows, start_time, skipped_tas, submission_attributes, reverse)

    update_c_to_d_linkages("contract", False, submission_attributes.submission_id)
    update_c_to_d_linkages("assistance", False, submission_attributes.submission_id)

    for key in skipped_tas:
        logger.info(f"Skipped {skipped_tas[key]['count']:,} rows due to missing TAS: {key}")

    total_tas_skipped = 0
    for key in skipped_tas:
        total_tas_skipped += skipped_tas[key]["count"]

    logger.info(f"Skipped a total of {total_tas_skipped:,} TAS rows for File C")


def _save_file_c_rows(certified_award_financial, total_rows, start_time, skipped_tas, submission_attributes, reverse):
    save_manager = BulkCreateManager(FinancialAccountsByAwards)
    for index, row in enumerate(certified_award_financial, 1):
        if not (index % 1000):
            logger.info(f"C File Load: Loading row {index:,} of {total_rows:,} ({datetime.now() - start_time})")

        upper_case_dict_values(row)

        # Check and see if there is an entry for this TAS
        treasury_account, tas_rendering_label = get_treasury_appropriation_account_tas_lookup(row.get("tas_id"))
        if treasury_account is None:
            update_skipped_tas(row, tas_rendering_label, skipped_tas)
            continue

        award_financial_data = FinancialAccountsByAwards()

        value_map_faba = {
            "submission": submission_attributes,
            "reporting_period_start": submission_attributes.reporting_period_start,
            "reporting_period_end": submission_attributes.reporting_period_end,
            "treasury_account": treasury_account,
            "object_class": row.get("object_class"),
            "program_activity": row.get("program_activity"),
            "disaster_emergency_fund": get_disaster_emergency_fund(row),
            "distinct_award_key": create_distinct_award_key(row),
        }

        save_manager.append(
            load_data_into_model(award_financial_data, row, value_map=value_map_faba, save=False, reverse=reverse)
        )

    save_manager.save_stragglers()


def create_distinct_award_key(row):
    parent_award_id = row.get("parent_award_id") or ""
    return f"{row.get('piid') or ''}|{parent_award_id}|{row.get('fain') or ''}|{row.get('uri') or ''}".upper()
