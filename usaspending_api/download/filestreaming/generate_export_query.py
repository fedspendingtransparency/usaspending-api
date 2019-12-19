from django.conf import settings


def generate_default_export_query(source, inner_sql, export_options):
    return r"\COPY ({}) TO STDOUT {}".format(inner_sql, export_options)


def generate_file_b_custom_account_download_export_query(source, inner_sql, export_options):
    """
    DEV-3997 requests that we eliminate excess $0 sum rows and roll up missing direct/reimbursable
    rows where possible in custom account File B (a.k.a. object class program activity) downloads.
    While it may be possible to achieve this using the ORM, a solution eluded me so I dropped back
    wrapping the download query in SQL that will achieve the requirements.
    """
    tas_or_fas = "federal_account_symbol" if source.file_type == "federal_account" else "treasury_account_symbol"
    export_sql_file = settings.APP_DIR / "download" / "filestreaming" / "file_b_custom_account_download_export.sql"
    export_sql = export_sql_file.read_text()
    return export_sql.format(tas_or_fas=tas_or_fas, inner_sql=inner_sql, export_options=export_options)
