import copy
import pytest

from datetime import datetime, timedelta

from django.conf import settings
from django.core.management import call_command
from django.db import connections
from django.db.models import Q
from django.test import TestCase
from model_bakery import baker
from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.etl.submission_loader_helpers.object_class import reset_object_class_cache
from usaspending_api.etl.transaction_loaders.data_load_helpers import format_insert_or_update_column_sql

earlier_time = datetime.now() - timedelta(days=1)
current_time = datetime.now()


@pytest.mark.usefixtures("broker_db_setup", "broker_server_dblink_setup")
class TestWithMultipleDatabases(TestCase):
    databases = "__all__"

    @classmethod
    def setUpTestData(cls):

        reset_object_class_cache()

        baker.make(
            "accounts.TreasuryAppropriationAccount",
            treasury_account_identifier=-99999,
            allocation_transfer_agency_id="999",
            agency_id="999",
            beginning_period_of_availability="1700-01-01",
            ending_period_of_availability="1700-12-31",
            availability_type_code="000",
            main_account_code="0000",
            sub_account_code="0000",
            tas_rendering_label="1004-1002-1003-1007-1008",
        )
        baker.make(
            "references.ObjectClass", id=0, major_object_class="00", object_class="00.0", direct_reimbursable=None
        )
        baker.make(
            "submissions.DABSSubmissionWindowSchedule",
            id="2000060",
            submission_fiscal_year=0,
            submission_fiscal_month=0,
            submission_fiscal_quarter=0,
            is_quarter=False,
        )

        # Setup default data in Broker Test DB
        broker_objects_to_insert = {
            "tas_lookup": {"broker_object": _assemble_broker_tas_lookup_records(), "conflict_column": "tas_id"},
            "submission": {"broker_object": _assemble_broker_submission_records(), "conflict_column": "submission_id"},
            "publish_history": {"broker_object": _assemble_publish_history(), "conflict_column": "publish_history_id"},
            "certify_history": {"broker_object": _assemble_certify_history(), "conflict_column": "certify_history_id"},
            "published_files_history": {
                "broker_object": _assemble_published_files_history(),
                "conflict_column": "published_files_history_id",
            },
            "published_award_financial": {
                "broker_object": _assemble_published_award_financial_records(),
                "conflict_column": "published_award_financial_id",
            },
        }
        connection = connections[settings.DATA_BROKER_DB_ALIAS]
        with connection.cursor() as cursor:

            for broker_table_name, value in broker_objects_to_insert.items():
                broker_object = value["broker_object"]
                conflict_column = value["conflict_column"]
                for load_object in [dict(**{broker_table_name: obj}) for obj in broker_object]:
                    columns, values, pairs = format_insert_or_update_column_sql(cursor, load_object, broker_table_name)
                    insert_sql = (
                        f"INSERT INTO {broker_table_name} {columns} VALUES {values}"
                        f" ON CONFLICT ({conflict_column}) DO UPDATE SET {pairs};"
                    )
                    cursor.execute(insert_sql)

    @pytest.mark.signal_handling  # see mark doc in pyproject.toml
    def test_load_submission_transaction_obligated_amount(self):
        """Test load submission management command for File C transaction_obligated_amount 0 and NULL values"""
        call_command("load_submission", "-9999")

        expected_results = 0
        actual_results = FinancialAccountsByAwards.objects.filter(
            Q(transaction_obligated_amount__isnull=True) | Q(transaction_obligated_amount=0)
        ).count()

        assert expected_results == actual_results

    @pytest.mark.signal_handling  # see mark doc in pyproject.toml
    def test_load_submission_file_c_fain_and_uri(self):
        """
        Test load submission management command for File C records with FAIN and URI
        """
        baker.make(
            "search.AwardSearch",
            award_id=-999,
            uri="RANDOM_LOAD_SUB_URI_999",
            fain="RANDOM_LOAD_SUB_FAIN_999",
            latest_transaction_id=-999,
        )
        baker.make(
            "search.AwardSearch",
            award_id=-1999,
            uri="RANDOM_LOAD_SUB_URI_1999",
            fain="RANDOM_LOAD_SUB_FAIN_1999",
            latest_transaction_id=-1999,
        )
        baker.make("search.TransactionSearch", transaction_id=-999)
        baker.make("search.TransactionSearch", transaction_id=-1999)

        # Test loading submission with File C to D Linkage
        call_command("load_submission", "-9999")

        expected_results = {"award_ids": [-1999, -999]}
        actual_results = {
            "award_ids": sorted(
                list(
                    FinancialAccountsByAwards.objects.filter(award_id__isnull=False).values_list("award_id", flat=True)
                )
            )
        }

        assert expected_results == actual_results

        # Call with Skipping C to D Linkage
        call_command("load_submission", "-9999", "--force-reload", "--skip-c-to-d-linkage")

        expected_results = {"award_ids": []}
        actual_results = {
            "award_ids": sorted(
                list(
                    FinancialAccountsByAwards.objects.filter(award_id__isnull=False).values_list("award_id", flat=True)
                )
            )
        }

        assert expected_results == actual_results

    @pytest.mark.signal_handling  # see mark doc in pyproject.toml
    def test_load_submission_file_c_uri(self):
        """
        Test load submission management command for File C records with only a URI
        """
        baker.make("search.AwardSearch", award_id=-997, uri="RANDOM_LOAD_SUB_URI", latest_transaction_id=-997)
        baker.make("search.TransactionSearch", transaction_id=-997)

        # Test loading submission with File C to D Linkage
        call_command("load_submission", "-9999")

        expected_results = {"award_ids": [-997]}
        actual_results = {
            "award_ids": list(
                FinancialAccountsByAwards.objects.filter(award_id__isnull=False).values_list("award_id", flat=True)
            )
        }

        assert expected_results == actual_results

        # Call with Skipping C to D Linkage
        call_command("load_submission", "-9999", "--force-reload", "--skip-c-to-d-linkage")

        expected_results = {"award_ids": []}
        actual_results = {
            "award_ids": list(
                FinancialAccountsByAwards.objects.filter(award_id__isnull=False).values_list("award_id", flat=True)
            )
        }

        assert expected_results == actual_results

    @pytest.mark.signal_handling  # see mark doc in pyproject.toml
    def test_load_submission_file_c_fain(self):
        """
        Test load submission management command for File C records with only a FAIN
        """
        baker.make("search.AwardSearch", award_id=-997, fain="RANDOM_LOAD_SUB_FAIN", latest_transaction_id=-997)
        baker.make("search.TransactionSearch", transaction_id=-997)

        # Test loading submission with File C to D Linkage
        call_command("load_submission", "-9999")

        expected_results = {"award_ids": [-997]}
        actual_results = {
            "award_ids": list(
                FinancialAccountsByAwards.objects.filter(award_id__isnull=False).values_list("award_id", flat=True)
            )
        }

        assert expected_results == actual_results

        # Call with Skipping C to D Linkage
        call_command("load_submission", "-9999", "--force-reload", "--skip-c-to-d-linkage")

        expected_results = {"award_ids": []}
        actual_results = {
            "award_ids": list(
                FinancialAccountsByAwards.objects.filter(award_id__isnull=False).values_list("award_id", flat=True)
            )
        }

        assert expected_results == actual_results

    @pytest.mark.signal_handling  # see mark doc in pyproject.toml
    def test_load_submission_file_c_piid_with_parent_piid(self):
        """
        Test load submission management command for File C records with only a piid and parent piid
        """
        baker.make(
            "search.AwardSearch",
            award_id=-997,
            piid="RANDOM_LOAD_SUB_PIID",
            parent_award_piid="RANDOM_LOAD_SUB_PARENT_PIID",
            latest_transaction_id=-997,
        )
        baker.make("search.TransactionSearch", transaction_id=-997)

        # Test loading submission with File C to D Linkage
        call_command("load_submission", "-9999")

        expected_results = {"award_ids": [-997, -997]}
        actual_results = {
            "award_ids": list(
                FinancialAccountsByAwards.objects.filter(award_id__isnull=False).values_list("award_id", flat=True)
            )
        }

        assert expected_results == actual_results

        # Call with Skipping C to D Linkage
        call_command("load_submission", "-9999", "--force-reload", "--skip-c-to-d-linkage")

        expected_results = {"award_ids": []}
        actual_results = {
            "award_ids": list(
                FinancialAccountsByAwards.objects.filter(award_id__isnull=False).values_list("award_id", flat=True)
            )
        }

        assert expected_results == actual_results

    @pytest.mark.signal_handling  # see mark doc in pyproject.toml
    def test_load_submission_file_c_piid_with_no_parent_piid(self):
        """
        Test load submission management command for File C records with only a piid and no parent piid
        """
        baker.make(
            "search.AwardSearch",
            award_id=-998,
            piid="RANDOM_LOAD_SUB_PIID",
            parent_award_piid=None,
            latest_transaction_id=-998,
        )
        baker.make("search.TransactionSearch", transaction_id=-998)

        # Test loading submission with File C to D Linkage
        call_command("load_submission", "-9999")

        expected_results = {"award_ids": [-998]}
        actual_results = {
            "award_ids": list(
                FinancialAccountsByAwards.objects.filter(award_id__isnull=False).values_list("award_id", flat=True)
            )
        }

        assert expected_results == actual_results

        # Call with Skipping C to D Linkage
        call_command("load_submission", "-9999", "--force-reload", "--skip-c-to-d-linkage")

        expected_results = {"award_ids": []}
        actual_results = {
            "award_ids": list(
                FinancialAccountsByAwards.objects.filter(award_id__isnull=False).values_list("award_id", flat=True)
            )
        }

        assert expected_results == actual_results

    @pytest.mark.signal_handling  # see mark doc in pyproject.toml
    def test_load_submission_file_c_piid_with_unmatched_parent_piid(self):
        """
        Test load submission management command for File C records that are not expected to be linked to Award data
        """
        baker.make(
            "search.AwardSearch",
            award_id=-1001,
            piid="RANDOM_LOAD_SUB_PIID",
            parent_award_piid="PARENT_LOAD_SUB_PIID_DNE",
            latest_transaction_id=-1234,
        )
        baker.make("search.TransactionSearch", transaction_id=-1234)

        # Test loading submission with File C to D Linkage
        call_command("load_submission", "-9999")

        expected_results = {"award_ids": [-1001]}
        actual_results = {
            "award_ids": list(
                FinancialAccountsByAwards.objects.filter(award_id__isnull=False).values_list("award_id", flat=True)
            )
        }

        assert expected_results == actual_results

        # Call with Skiping C to D Linkage
        call_command("load_submission", "-9999", "--force-reload", "--skip-c-to-d-linkage")

        expected_results = {"award_ids": []}
        actual_results = {
            "award_ids": list(
                FinancialAccountsByAwards.objects.filter(award_id__isnull=False).values_list("award_id", flat=True)
            )
        }

        assert expected_results == actual_results

    @pytest.mark.signal_handling  # see mark doc in pyproject.toml
    def test_load_submission_file_c_no_d_linkage(self):
        """
        Test load submission management command for File C records that are not expected to be linked to Award data
        """
        baker.make(
            "search.AwardSearch",
            award_id=-999,
            piid="RANDOM_LOAD_SUB_PIID_DNE",
            parent_award_piid="PARENT_LOAD_SUB_PIID_DNE",
            latest_transaction_id=-999,
        )
        baker.make("search.TransactionSearch", transaction_id=-999, award_id=-999)

        call_command("load_submission", "-9999")

        expected_results = {"award_ids": []}
        actual_results = {
            "award_ids": list(
                FinancialAccountsByAwards.objects.filter(award_id__isnull=False).values_list("award_id", flat=True)
            )
        }

        assert expected_results == actual_results

    @pytest.mark.signal_handling  # see mark doc in pyproject.toml
    def test_load_submission_file_c_zero_and_null_amount_rows_ignored(self):
        """
        Test that 'published_award_financial` rows that have a zero or null for both
        'transaction_obligated_amou' and 'gross_outlay_amount_by_awa_cpe' are not loaded  from Broker.
        """
        call_command("load_submission", "-9999")

        assert FinancialAccountsByAwards.objects.all().count() == 6


def _assemble_broker_tas_lookup_records() -> list:
    base_record = {
        "created_at": None,
        "updated_at": None,
        "tas_id": None,
        "allocation_transfer_agency": None,
        "agency_identifier": None,
        "beginning_period_of_availa": None,
        "ending_period_of_availabil": None,
        "availability_type_code": None,
        "main_account_code": None,
        "sub_account_code": None,
        "account_num": None,
        "internal_end_date": None,
        "internal_start_date": "2015-01-01",
        "financial_indicator2": None,
        "fr_entity_description": None,
        "fr_entity_type": None,
        "account_title": None,
        "budget_bureau_code": None,
        "budget_bureau_name": None,
        "budget_function_code": None,
        "budget_function_title": None,
        "budget_subfunction_code": None,
        "budget_subfunction_title": None,
        "reporting_agency_aid": None,
        "reporting_agency_name": None,
    }
    # TODO: Review this section to be more aligned with the actual data
    default_tas_lookup_record = copy.copy(base_record)
    default_tas_lookup_record["tas"] = "1001100210051006100310071008"
    default_tas_lookup_record["display_tas"] = "1001-1002-1005/1006-1007-1008"
    default_tas_lookup_record["tas_id"] = -999
    default_tas_lookup_record["account_num"] = -99999
    default_tas_lookup_record["allocation_transfer_agency"] = 1001
    default_tas_lookup_record["agency_identifier"] = 1002
    default_tas_lookup_record["availability_type_code"] = 1003
    default_tas_lookup_record["allocation_transfer_agency"] = 1004
    default_tas_lookup_record["beginning_period_of_availa"] = 1005
    default_tas_lookup_record["ending_period_of_availabil"] = 1006
    default_tas_lookup_record["main_account_code"] = 1007
    default_tas_lookup_record["sub_account_code"] = 1008

    return [default_tas_lookup_record]


def _assemble_broker_submission_records() -> list:
    base_record = {
        "created_at": None,
        "updated_at": None,
        "submission_id": None,
        "user_id": None,
        "cgac_code": None,
        "reporting_start_date": None,
        "reporting_end_date": None,
        "is_quarter_format": False,
        "number_of_errors": 0,
        "number_of_warnings": 0,
        "publish_status_id": 2,
        "publishable": False,
        "reporting_fiscal_period": 0,
        "reporting_fiscal_year": 0,
        "is_fabs": False,
        "publishing_user_id": None,
        "frec_code": None,
    }

    default_submission_record = copy.copy(base_record)
    default_submission_record["submission_id"] = -9999

    return [default_submission_record]


def _assemble_published_award_financial_records() -> list:
    base_record = {
        "created_at": None,
        "updated_at": None,
        "published_award_financial_id": None,
        "submission_id": -9999,
        "job_id": None,
        "row_number": None,
        "agency_identifier": None,
        "allocation_transfer_agency": None,
        "availability_type_code": None,
        "beginning_period_of_availa": None,
        "by_direct_reimbursable_fun": None,
        "deobligations_recov_by_awa_cpe": None,
        "ending_period_of_availabil": None,
        "fain": None,
        "gross_outlay_amount_by_awa_cpe": None,
        "gross_outlay_amount_by_awa_fyb": None,
        "gross_outlays_delivered_or_cpe": None,
        "gross_outlays_delivered_or_fyb": None,
        "gross_outlays_undelivered_cpe": None,
        "gross_outlays_undelivered_fyb": None,
        "main_account_code": None,
        "object_class": "000",
        "obligations_delivered_orde_cpe": None,
        "obligations_delivered_orde_fyb": None,
        "obligations_incurred_byawa_cpe": None,
        "obligations_undelivered_or_cpe": None,
        "obligations_undelivered_or_fyb": None,
        "parent_award_id": None,
        "piid": None,
        "program_activity_code": None,
        "program_activity_name": None,
        "sub_account_code": None,
        "transaction_obligated_amou": 100,
        "uri": None,
        "ussgl480100_undelivered_or_cpe": None,
        "ussgl480100_undelivered_or_fyb": None,
        "ussgl480200_undelivered_or_cpe": None,
        "ussgl480200_undelivered_or_fyb": None,
        "ussgl483100_undelivered_or_cpe": None,
        "ussgl483200_undelivered_or_cpe": None,
        "ussgl487100_downward_adjus_cpe": None,
        "ussgl487200_downward_adjus_cpe": None,
        "ussgl488100_upward_adjustm_cpe": None,
        "ussgl488200_upward_adjustm_cpe": None,
        "ussgl490100_delivered_orde_cpe": None,
        "ussgl490100_delivered_orde_fyb": None,
        "ussgl490200_delivered_orde_cpe": None,
        "ussgl490800_authority_outl_cpe": None,
        "ussgl490800_authority_outl_fyb": None,
        "ussgl493100_delivered_orde_cpe": None,
        "ussgl497100_downward_adjus_cpe": None,
        "ussgl497200_downward_adjus_cpe": None,
        "ussgl498100_upward_adjustm_cpe": None,
        "ussgl498200_upward_adjustm_cpe": None,
        "tas": None,
        "account_num": -99999,
    }

    row_with_piid_and_no_parent_piid = copy.copy(base_record)
    row_with_piid_and_no_parent_piid["published_award_financial_id"] = 1
    row_with_piid_and_no_parent_piid["piid"] = "RANDOM_LOAD_SUB_PIID"

    row_with_piid_and_parent_piid = copy.copy(base_record)
    row_with_piid_and_parent_piid["published_award_financial_id"] = 2
    row_with_piid_and_parent_piid["piid"] = "RANDOM_LOAD_SUB_PIID"
    row_with_piid_and_parent_piid["parent_award_id"] = "RANDOM_LOAD_SUB_PARENT_PIID"

    row_with_fain = copy.copy(base_record)
    row_with_fain["published_award_financial_id"] = 3
    row_with_fain["fain"] = "RANDOM_LOAD_SUB_FAIN"

    row_with_uri = copy.copy(base_record)
    row_with_uri["published_award_financial_id"] = 4
    row_with_uri["uri"] = "RANDOM_LOAD_SUB_URI"

    row_with_fain_and_uri_dne = copy.copy(base_record)
    row_with_fain_and_uri_dne["published_award_financial_id"] = 5
    row_with_fain_and_uri_dne["fain"] = "RANDOM_LOAD_SUB_FAIN_999"
    row_with_fain_and_uri_dne["uri"] = "RANDOM_LOAD_SUB_URI_DNE"

    row_with_uri_and_fain_dne = copy.copy(base_record)
    row_with_uri_and_fain_dne["published_award_financial_id"] = 6
    row_with_uri_and_fain_dne["fain"] = "RANDOM_LOAD_SUB_FAIN_DNE"
    row_with_uri_and_fain_dne["uri"] = "RANDOM_LOAD_SUB_URI_1999"

    row_with_zero_transaction_obligated_amount = copy.copy(base_record)
    row_with_zero_transaction_obligated_amount["published_award_financial_id"] = 7
    row_with_zero_transaction_obligated_amount["transaction_obligated_amou"] = 0

    row_with_null_transaction_obligated_amount = copy.copy(base_record)
    row_with_null_transaction_obligated_amount["published_award_financial_id"] = 8
    row_with_null_transaction_obligated_amount["transaction_obligated_amou"] = None

    return [
        row_with_piid_and_no_parent_piid,
        row_with_piid_and_parent_piid,
        row_with_fain,
        row_with_uri,
        row_with_fain_and_uri_dne,
        row_with_uri_and_fain_dne,
        row_with_zero_transaction_obligated_amount,
        row_with_null_transaction_obligated_amount,
    ]


def _assemble_publish_history():
    base_record = {
        "created_at": earlier_time,
        "updated_at": earlier_time,
        "publish_history_id": 1,
        "submission_id": -9999,
        "user_id": None,
    }
    return [base_record]


def _assemble_certify_history():
    base_record = {
        "created_at": current_time,
        "updated_at": current_time,
        "certify_history_id": 1,
        "submission_id": -9999,
        "user_id": None,
    }
    return [base_record]


def _assemble_published_files_history():
    base_record = {
        "created_at": None,
        "updated_at": None,
        "published_files_history_id": 1,
        "publish_history_id": 1,
        "certify_history_id": None,
        "submission_id": -9999,
    }
    return [base_record]
