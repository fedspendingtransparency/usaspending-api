# Manually created to support stage and swap of renamed columns

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ("financial_activities", "0011_alter_financialaccountsbyprogramactivityobjectclass_pa_reporting_key"),
    ]

    operations = [
        migrations.AddField(
            model_name="financialaccountsbyprogramactivityobjectclass",
            name='program_activity_reporting_key',
            field=models.TextField(blank=True, help_text="A unique identifier for a Program Activity", null=True),
        ),
        migrations.AddField(
            model_name="financialaccountsbyprogramactivityobjectclass",
            name='ussgl480110_rein_undel_ord_cpe',
            field=models.DecimalField(blank=True, decimal_places=2, max_digits=23, null=True),
        ),
        migrations.AddField(
            model_name="financialaccountsbyprogramactivityobjectclass",
            name='ussgl490110_rein_deliv_ord_cpe',
            field=models.DecimalField(blank=True, decimal_places=2, max_digits=23, null=True),
        ),
    ]
