# Manually created to support stage and swap of renamed columns

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
    ]
