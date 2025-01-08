# Manually created to support stage and swap of renamed columns

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
    ]
