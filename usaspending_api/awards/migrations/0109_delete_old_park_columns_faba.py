# Manually created to support stage and swap of renamed columns
from pathlib import Path

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("awards", "0108_stage_renamed_park_columns_faba"),
    ]

    operations = [
        migrations.RemoveField(
            model_name="financialaccountsbyawards",
            name="pa_reporting_key",
        ),
        migrations.RemoveField(
            model_name="financialaccountsbyawards",
            name="ussgl480110_reinstated_del_cpe",
        ),
        migrations.RemoveField(
            model_name="financialaccountsbyawards",
            name="ussgl490110_reinstated_del_cpe",
        ),
        # Need to recreate view after columns were dropped
        migrations.RunSQL(
            sql=[f"{Path('usaspending_api/download/sql/vw_financial_accounts_by_awards_download.sql').read_text()}"],
            reverse_sql=["DROP VIEW IF EXISTS vw_financial_accounts_by_awards_download;"],
        ),
    ]
