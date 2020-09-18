from django.conf import settings
from django.db import connection


def verify_database_view_exists(view_name: str) -> bool:
    """Check if the view exists in the target database"""
    list_views = "SELECT table_name FROM INFORMATION_SCHEMA.views WHERE table_schema = ANY(current_schemas(false))"
    with connection.cursor() as cursor:
        cursor.execute(list_views)
        list_of_available_views = [row[0] for row in cursor.fetchall()]

    return view_name in list_of_available_views


def ensure_view_exists(view_name: str, force: bool = False):
    view_file_path = settings.APP_DIR / "database_scripts" / "etl" / f"{view_name}.sql"

    if verify_database_view_exists(view_name) and not force:
        return

    view_sql = view_file_path.read_text()
    with connection.cursor() as cursor:
        cursor.execute(view_sql)


def ensure_business_categories_functions_exist():
    file_path = settings.APP_DIR / "broker" / "management" / "sql" / "create_business_categories_functions.sql"
    sql = file_path.read_text()
    with connection.cursor() as cursor:
        cursor.execute(sql)
