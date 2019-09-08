from collections import OrderedDict
from pathlib import Path
from django.conf import settings

from usaspending_api.accounts.models import TASAutocompleteMatview

import usaspending_api.awards.models_matviews as mv

APP_DIR = Path(settings.BASE_DIR).resolve() / "usaspending_api"
DEFAULT_MATIVEW_DIR = Path(settings.BASE_DIR).resolve().parent / "matviews"
DEPENDENCY_FILES = [APP_DIR / "database_scripts/matviews/functions_and_enums.sql"]
JSON_DIR = APP_DIR / "database_scripts/matview_sql_generator"
MATVIEW_GENERATOR_FILE = APP_DIR / "database_scripts/matview_generator/matview_sql_generator.py"
OVERLAY_VIEWS = [APP_DIR / "database_scripts/matviews/vw_award_search.sql"]
DROP_OLD_MATVIEWS = APP_DIR / "database_scripts/matviews/drop_old_matviews.sql"
MATERIALIZED_VIEWS = OrderedDict(
    [
        (
            "mv_award_summary",
            {
                "model": mv.AwardSummaryMatview,
                "json_filepath": str(JSON_DIR / "mv_award_summary.json"),
                "sql_filename": "mv_award_summary.sql",
            },
        ),
        (
            "mv_contract_award_search",
            {
                "model": mv.ContractAwardSearchMatview,
                "json_filepath": str(JSON_DIR / "mv_contract_award_search.json"),
                "sql_filename": "mv_contract_award_search.sql",
            },
        ),
        (
            "mv_directpayment_award_search",
            {
                "model": mv.DirectPaymentAwardSearchMatview,
                "json_filepath": str(JSON_DIR / "mv_directpayment_award_search.json"),
                "sql_filename": "mv_directpayment_award_search.sql",
            },
        ),
        (
            "mv_grant_award_search",
            {
                "model": mv.GrantAwardSearchMatview,
                "json_filepath": str(JSON_DIR / "mv_grant_award_search.json"),
                "sql_filename": "mv_grant_award_search.sql",
            },
        ),
        (
            "mv_idv_award_search",
            {
                "model": mv.IDVAwardSearchMatview,
                "json_filepath": str(JSON_DIR / "mv_idv_award_search.json"),
                "sql_filename": "mv_idv_award_search.sql",
            },
        ),
        (
            "mv_loan_award_search",
            {
                "model": mv.LoanAwardSearchMatview,
                "json_filepath": str(JSON_DIR / "mv_loan_award_search.json"),
                "sql_filename": str(DEFAULT_MATIVEW_DIR / "mv_loan_award_search.sql"),
            },
        ),
        (
            "mv_other_award_search",
            {
                "model": mv.OtherAwardSearchMatview,
                "json_filepath": str(JSON_DIR / "mv_other_award_search.json"),
                "sql_filename": "mv_other_award_search.sql",
            },
        ),
        (
            "mv_pre2008_award_search",
            {
                "model": mv.Pre2008AwardSearchMatview,
                "json_filepath": str(JSON_DIR / "mv_pre2008_award_search.json"),
                "sql_filename": "mv_pre2008_award_search.sql",
            },
        ),
        (
            "subaward_view",
            {
                "model": mv.SubawardView,
                "json_filepath": str(JSON_DIR / "subaward_view.json"),
                "sql_filename": "subaward_view.sql",
            },
        ),
        (
            "summary_state_view",
            {
                "model": mv.SummaryStateView,
                "json_filepath": str(JSON_DIR / "summary_state_view.json"),
                "sql_filename": "summary_state_view.sql",
            },
        ),
        (
            "summary_transaction_fed_acct_view",
            {
                "model": mv.SummaryTransactionFedAcctView,
                "json_filepath": str(JSON_DIR / "summary_transaction_fed_acct_view.json"),
                "sql_filename": "summary_transaction_fed_acct_view.sql",
            },
        ),
        (
            "summary_transaction_geo_view",
            {
                "model": mv.SummaryTransactionGeoView,
                "json_filepath": str(JSON_DIR / "summary_transaction_geo_view.json"),
                "sql_filename": "summary_transaction_geo_view.sql",
            },
        ),
        (
            "summary_transaction_month_view",
            {
                "model": mv.SummaryTransactionMonthView,
                "json_filepath": str(JSON_DIR / "summary_transaction_month_view.json"),
                "sql_filename": "summary_transaction_month_view.sql",
            },
        ),
        (
            "summary_transaction_recipient_view",
            {
                "model": mv.SummaryTransactionRecipientView,
                "json_filepath": str(JSON_DIR / "summary_transaction_recipient_view.json"),
                "sql_filename": "summary_transaction_recipient_view.sql",
            },
        ),
        (
            "summary_transaction_view",
            {
                "model": mv.SummaryTransactionView,
                "json_filepath": str(JSON_DIR / "summary_transaction_view.json"),
                "sql_filename": "summary_transaction_view.sql",
            },
        ),
        (
            "summary_view",
            {
                "model": mv.SummaryView,
                "json_filepath": str(JSON_DIR / "summary_view.json"),
                "sql_filename": "summary_view.sql",
            },
        ),
        (
            "summary_view_cfda_number",
            {
                "model": mv.SummaryCfdaNumbersView,
                "json_filepath": str(JSON_DIR / "summary_view_cfda_number.json"),
                "sql_filename": "summary_view_cfda_number.sql",
            },
        ),
        (
            "summary_view_naics_codes",
            {
                "model": mv.SummaryNaicsCodesView,
                "json_filepath": str(JSON_DIR / "summary_view_naics_codes.json"),
                "sql_filename": "summary_view_naics_codes.sql",
            },
        ),
        (
            "summary_view_psc_codes",
            {
                "model": mv.SummaryPscCodesView,
                "json_filepath": str(JSON_DIR / "summary_view_psc_codes.json"),
                "sql_filename": "summary_view_psc_codes.sql",
            },
        ),
        (
            "tas_autocomplete_matview",
            {
                "model": TASAutocompleteMatview,
                "json_filepath": str(JSON_DIR / "tas_autocomplete_matview.json"),
                "sql_filename": "tas_autocomplete_matview.sql",
            },
        ),
        (
            "universal_transaction_matview",
            {
                "model": mv.UniversalTransactionView,
                "json_filepath": str(JSON_DIR / "universal_transaction_matview.json"),
                "sql_filename": "universal_transaction_matview.sql",
            },
        ),
    ]
)
