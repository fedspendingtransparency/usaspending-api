# Manually created to support stage and swap of renamed columns
from pathlib import Path

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("financial_activities", "0012_stage_renamed_park_columns_fabpaoc"),
    ]

    operations = [
        migrations.RemoveField(
            model_name="financialaccountsbyprogramactivityobjectclass",
            name="pa_reporting_key",
        ),
        migrations.RemoveField(
            model_name="financialaccountsbyprogramactivityobjectclass",
            name="ussgl480110_reinstated_del_cpe",
        ),
        migrations.RemoveField(
            model_name="financialaccountsbyprogramactivityobjectclass",
            name="ussgl490110_reinstated_del_cpe",
        ),
        # Need to recreate view after columns were dropped
        migrations.RunSQL(
            sql=[f"{Path('usaspending_api/download/sql/vw_financial_accounts_by_program_activity_object_class_download.sql').read_text()}"],
            reverse_sql=["DROP VIEW IF EXISTS vw_financial_accounts_by_program_activity_object_class_download;"],
        ),
    ]
