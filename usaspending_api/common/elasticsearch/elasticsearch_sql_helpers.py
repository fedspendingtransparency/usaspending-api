from django.conf import settings
from django.db import connection


def verify_database_view_exists(view_name: str) -> bool:
    """Check if the view exists in the target database"""
    list_views = "SELECT table_name FROM INFORMATION_SCHEMA.views WHERE table_schema = ANY(current_schemas(false))"
    with connection.cursor() as cursor:
        cursor.execute(list_views)
        list_of_available_views = [row[0] for row in cursor.fetchall()]

    return view_name in list_of_available_views


def ensure_transaction_etl_view_exists(force: bool = False) -> None:
    """
    The view is used to populate the Elasticsearch transaction index.
    This function will ensure the view exists in the database.
    """

    if verify_database_view_exists(settings.ES_TRANSACTIONS_ETL_VIEW_NAME) and not force:
        return

    view_file_path = settings.APP_DIR / "database_scripts" / "etl" / "transaction_delta_view.sql"

    view_sql = view_file_path.read_text()
    with connection.cursor() as cursor:
        cursor.execute(view_sql)
