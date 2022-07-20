from collections import OrderedDict
from django.conf import settings

from usaspending_api.search.models import TASAutocompleteMatview

import usaspending_api.search.models as mv

DEFAULT_MATIVEW_DIR = settings.REPO_DIR / "matviews"
DEFAULT_CHUNKED_MATIVEW_DIR = settings.REPO_DIR / "chunked_matviews"
DEPENDENCY_FILEPATH = settings.APP_DIR / "database_scripts" / "matviews" / "functions_and_enums.sql"
JSON_DIR = settings.APP_DIR / "database_scripts" / "matview_generator"
MATVIEW_GENERATOR_FILE = settings.APP_DIR / "database_scripts" / "matview_generator" / "matview_sql_generator.py"
CHUNKED_MATVIEW_GENERATOR_FILE = (
    settings.APP_DIR / "database_scripts" / "matview_generator" / "chunked_matview_sql_generator.py"
)
OVERLAY_VIEWS = [
    settings.APP_DIR / "database_scripts" / "matviews" / "vw_award_search.sql",
    settings.APP_DIR / "database_scripts" / "matviews" / "vw_es_award_search.sql",
]
DROP_OLD_MATVIEWS = settings.APP_DIR / "database_scripts" / "matviews" / "drop_old_matviews.sql"
MATERIALIZED_VIEWS = OrderedDict(
    [
        (
            "mv_agency_autocomplete",
            {
                "model": mv.AgencyAutocompleteMatview,
                "json_filepath": str(JSON_DIR / "mv_agency_autocomplete.json"),
                "sql_filename": "mv_agency_autocomplete.sql",
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
                "sql_filename": "mv_loan_award_search.sql",
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
            "tas_autocomplete_matview",
            {
                "model": TASAutocompleteMatview,
                "json_filepath": str(JSON_DIR / "tas_autocomplete_matview.json"),
                "sql_filename": "tas_autocomplete_matview.sql",
            },
        ),
    ]
)
CHUNKED_MATERIALIZED_VIEWS = OrderedDict(
    [
        (
            "transaction_search",
            {
                "model": mv.TransactionSearch,
                "json_filepath": str(JSON_DIR / "transaction_search.json"),
                "sql_filename": "transaction_search.sql",
            },
        ),
        (
            "award_search",
            {
                "model": mv.AwardSearch,
                "json_filepath": str(JSON_DIR / "award_search.json"),
                "sql_filename": "award_search.sql",
            },
        ),
    ]
)
