from django.db import migrations, models

from usaspending_api.awards.models.award import vw_awards_sql
from usaspending_api.awards.models.transaction_fabs import vw_transaction_fabs_sql
from usaspending_api.awards.models.transaction_fpds import vw_transaction_fpds_sql
from usaspending_api.awards.models.transaction_normalized import (
    vw_transaction_normalized_sql,
)

transaction_delta_view_file = (
    "usaspending_api/database_scripts/etl/transaction_delta_view.sql"
)
with open(transaction_delta_view_file, "r") as f:
    transaction_delta_view = f.read()


class Migration(migrations.Migration):
    dependencies = [
        ("search", "0060_populate_datetimes"),
    ]

    operations = [
        migrations.RemoveField(
            model_name="transactionsearch",
            name="initial_report_date",
        ),
        migrations.RemoveField(
            model_name="transactionsearch",
            name="last_modified_date",
        ),
        migrations.RemoveField(
            model_name="awardsearch",
            name="last_modified_date",
        ),
        migrations.RenameField(
            model_name="transactionsearch",
            old_name="initial_report_datetime",
            new_name="initial_report_date",
        ),
        migrations.RenameField(
            model_name="transactionsearch",
            old_name="last_modified_datetime",
            new_name="last_modified_date",
        ),
        migrations.RenameField(
            model_name="awardsearch",
            old_name="last_modified_datetime",
            new_name="last_modified_date",
        ),
        migrations.AddIndex(
            model_name="transactionsearch",
            index=models.Index(
                fields=["-last_modified_date"], name="ts_idx_last_modified_date"
            ),
        ),
        migrations.RunSQL(
            sql=f"""
                {vw_awards_sql}
                {vw_transaction_normalized_sql}
                {vw_transaction_fpds_sql}
                {vw_transaction_fabs_sql}
                {transaction_delta_view}
            """,
            reverse_sql="""DROP VIEW IF EXISTS
                vw_awards,
                vw_transaction_fabs,
                vw_transaction_normalized,
                vw_transaction_fpds,
                transaction_delta_view
                """,
        ),
    ]
