from pathlib import Path
from django.conf import settings

from usaspending_api.accounts.models import TASAutocompleteMatview

import usaspending_api.awards.models_matviews as mv

MATVIEW_GENERATOR_FILE = (
    Path(settings.BASE_DIR).resolve() / "database_scripts/matview_generator/matview_sql_generator.py"
)
DEPENDENCY_FILES = [Path(settings.BASE_DIR).resolve() / "database_scripts/matviews/functions_and_enums.sql"]
OVERLAY_VIEWS = [Path(settings.BASE_DIR).resolve() / "database_scripts/matviews/vw_award_search.sql"]
DEFAULT_MATIVEW_DIR = Path(settings.BASE_DIR).resolve() / "matviews"
JSON_DIR = Path(settings.BASE_DIR).resolve() / "usaspending_api/database_scripts/matview_sql_generator"

MATERIALIZED_VIEWS = {
    "mv_award_summary": {
        "model": mv.AwardSummaryMatview,
        "json_file": str(JSON_DIR / "mv_award_summary.json"),
        "default_location": "{}/mv_award_summary.sql".format(DEFAULT_MATIVEW_DIR),
    },
    "mv_contract_award_search": {
        "model": mv.ContractAwardSearchMatview,
        "json_file": str(JSON_DIR / "mv_contract_award_search.json"),
        "default_location": str(DEFAULT_MATIVEW_DIR / "mv_contract_award_search.sql"),
    },
    "mv_directpayment_award_search": {
        "model": mv.DirectPaymentAwardSearchMatview,
        "json_file": str(JSON_DIR / "mv_directpayment_award_search.json"),
        "default_location": str(DEFAULT_MATIVEW_DIR / "mv_directpayment_award_search.sql"),
    },
    "mv_grant_award_search": {
        "model": mv.GrantAwardSearchMatview,
        "json_file": str(JSON_DIR / "mv_grant_award_search.json"),
        "default_location": str(DEFAULT_MATIVEW_DIR / "mv_grant_award_search.sql"),
    },
    "mv_idv_award_search": {
        "model": mv.IDVAwardSearchMatview,
        "json_file": str(JSON_DIR / "mv_idv_award_search.json"),
        "default_location": str(DEFAULT_MATIVEW_DIR / "mv_idv_award_search.sql"),
    },
    "mv_loan_award_search": {
        "model": mv.LoanAwardSearchMatview,
        "json_file": str(JSON_DIR / "mv_loan_award_search.json"),
        "default_location": str(DEFAULT_MATIVEW_DIR / "mv_loan_award_search.sql"),
    },
    "mv_other_award_search": {
        "model": mv.OtherAwardSearchMatview,
        "json_file": str(JSON_DIR / "mv_other_award_search.json"),
        "default_location": str(DEFAULT_MATIVEW_DIR / "mv_other_award_search.sql"),
    },
    "mv_pre2008_award_search": {
        "model": mv.Pre2008AwardSearchMatview,
        "json_file": str(JSON_DIR / "mv_pre2008_award_search.json"),
        "default_location": str(DEFAULT_MATIVEW_DIR / "mv_pre2008_award_search.sql"),
    },
    "subaward_view": {
        "model": mv.SubawardView,
        "json_file": str(JSON_DIR / "subaward_view.json"),
        "default_location": str(DEFAULT_MATIVEW_DIR / "subaward_view.sql"),
    },
    "summary_state_view": {
        "model": mv.SummaryStateView,
        "json_file": str(JSON_DIR / "summary_state_view.json"),
        "default_location": str(DEFAULT_MATIVEW_DIR / "summary_state_view.sql"),
    },
    "summary_transaction_fed_acct_view": {
        "model": mv.SummaryTransactionFedAcctView,
        "json_file": str(JSON_DIR / "summary_transaction_fed_acct_view.json"),
        "default_location": str(DEFAULT_MATIVEW_DIR / "summary_transaction_fed_acct_view.sql"),
    },
    "summary_transaction_geo_view": {
        "model": mv.SummaryTransactionGeoView,
        "json_file": str(JSON_DIR / "summary_transaction_geo_view.json"),
        "default_location": str(DEFAULT_MATIVEW_DIR / "summary_transaction_geo_view.sql"),
    },
    "summary_transaction_month_view": {
        "model": mv.SummaryTransactionMonthView,
        "json_file": str(JSON_DIR / "summary_transaction_month_view.json"),
        "default_location": str(DEFAULT_MATIVEW_DIR / "summary_transaction_month_view.sql"),
    },
    "summary_transaction_recipient_view": {
        "model": mv.SummaryTransactionRecipientView,
        "json_file": str(JSON_DIR / "summary_transaction_recipient_view.json"),
        "default_location": str(DEFAULT_MATIVEW_DIR / "summary_transaction_recipient_view.sql"),
    },
    "summary_transaction_view": {
        "model": mv.SummaryTransactionView,
        "json_file": str(JSON_DIR / "summary_transaction_view.json"),
        "default_location": str(DEFAULT_MATIVEW_DIR / "summary_transaction_view.sql"),
    },
    "summary_view": {
        "model": mv.SummaryView,
        "json_file": str(JSON_DIR / "summary_view.json"),
        "default_location": str(DEFAULT_MATIVEW_DIR / "summary_view.sql"),
    },
    "summary_view_cfda_number": {
        "model": mv.SummaryCfdaNumbersView,
        "json_file": str(JSON_DIR / "summary_view_cfda_number.json"),
        "default_location": str(DEFAULT_MATIVEW_DIR / "summary_view_cfda_number.sql"),
    },
    "summary_view_naics_codes": {
        "model": mv.SummaryNaicsCodesView,
        "json_file": str(JSON_DIR / "summary_view_naics_codes.json"),
        "default_location": str(DEFAULT_MATIVEW_DIR / "summary_view_naics_codes.sql"),
    },
    "summary_view_psc_codes": {
        "model": mv.SummaryPscCodesView,
        "json_file": str(JSON_DIR / "summary_view_psc_codes.json"),
        "default_location": str(DEFAULT_MATIVEW_DIR / "summary_view_psc_codes.sql"),
    },
    "tas_autocomplete_matview": {
        "model": TASAutocompleteMatview,
        "json_file": str(JSON_DIR / "tas_autocomplete_matview.json"),
        "default_location": str(DEFAULT_MATIVEW_DIR / "tas_autocomplete_matview.sql"),
    },
    "universal_transaction_matview": {
        "model": mv.UniversalTransactionView,
        "json_file": str(JSON_DIR / "universal_transaction_matview.json"),
        "default_location": str(DEFAULT_MATIVEW_DIR / "universal_transaction_matview.sql"),
    },
}
