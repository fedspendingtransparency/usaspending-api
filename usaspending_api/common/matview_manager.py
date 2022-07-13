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
    ]
)
