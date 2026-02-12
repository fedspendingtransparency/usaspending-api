# Manually created to handle swap in place of datetime fields and the corresponding views

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
        ("search", "0059_add_datetime_fields_for_swap_in_place"),
    ]

    operations = [
        # Drop VIEWS that rely on the updated tables below
        migrations.RunSQL(
            sql="""
                DROP VIEW IF EXISTS
                vw_awards,
                vw_transaction_fabs,
                vw_transaction_normalized,
                vw_transaction_fpds,
                transaction_delta_view;
                """,
            reverse_sql=f"""
                    {vw_awards_sql}
                    {vw_transaction_normalized_sql}
                    {vw_transaction_fpds_sql}
                    {vw_transaction_fabs_sql}
                    {transaction_delta_view}
                """,
        ),

        # Make sure the old indexes are removed
        migrations.RemoveIndex(
            model_name="subawardsearch",
            name="ss_idx_last_modified_date",
        ),
        migrations.RemoveIndex(
            model_name="transactionsearch",
            name="ts_idx_last_modified_date",
        ),

        # Drop the old columns
        migrations.RemoveField(
            model_name="awardsearch",
            name="last_modified_date",
        ),
        migrations.RemoveField(
            model_name="subawardsearch",
            name="last_modified_date",
        ),
        migrations.RemoveField(
            model_name="transactionsearch",
            name="initial_report_date",
        ),
        migrations.RemoveField(
            model_name="transactionsearch",
            name="last_modified_date",
        ),

        # Rename the new columns
        migrations.RenameField(
            model_name="awardsearch",
            old_name="last_modified_date_new",
            new_name="last_modified_date",
        ),
        migrations.RenameField(
            model_name="subawardsearch",
            old_name="last_modified_date_new",
            new_name="last_modified_date",
        ),
        migrations.RenameField(
            model_name="transactionsearch",
            old_name="initial_report_date_new",
            new_name="initial_report_date",
        ),
        migrations.RenameField(
            model_name="transactionsearch",
            old_name="last_modified_date_new",
            new_name="last_modified_date",
        ),

        # Rename the new indexes to take the expected name
        migrations.RenameIndex(
            model_name='subawardsearch',
            old_name='ss_idx_last_modified_date_new',
            new_name='ss_idx_last_modified_date',
        ),
        migrations.RenameIndex(
            model_name='transactionsearch',
            old_name='ts_idx_last_modified_date_new',
            new_name='ts_idx_last_modified_date',
        ),

        # Prevent Django from trying to create additional migrations that would recreate the indexes
        # that we have renamed for the purpose of the swap in place.
        migrations.RunSQL(
            sql="",
            reverse_sql="",
            state_operations=[
                migrations.RemoveIndex(
                    model_name='subawardsearch',
                    name='ss_idx_last_modified_date',
                ),
                migrations.RemoveIndex(
                    model_name='transactionsearch',
                    name='ts_idx_last_modified_date',
                ),
                migrations.AddIndex(
                    model_name='subawardsearch',
                    index=models.Index(models.OrderBy(models.F('last_modified_date'), descending=True, nulls_last=True),
                                       name='ss_idx_last_modified_date'),
                ),
                migrations.AddIndex(
                    model_name='transactionsearch',
                    index=models.Index(fields=['-last_modified_date'], name='ts_idx_last_modified_date'),
                ),
            ]
        ),

        # Recreate the VIEWS that were initially dropped
        migrations.RunSQL(
            sql=f"""
                {vw_awards_sql}
                {vw_transaction_normalized_sql}
                {vw_transaction_fpds_sql}
                {vw_transaction_fabs_sql}
                {transaction_delta_view}
            """,
            reverse_sql="""
                DROP VIEW IF EXISTS
                    vw_awards,
                    vw_transaction_fabs,
                    vw_transaction_normalized,
                    vw_transaction_fpds,
                    transaction_delta_view
            """,
        ),
    ]
