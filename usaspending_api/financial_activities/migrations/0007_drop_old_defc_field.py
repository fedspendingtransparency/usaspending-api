# Manually created to handle zero downtime with SQL views while changing DEFC column type
# JIRA Ticket: DEV-7953

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('financial_activities', '0006_use_new_defc_text_field'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='financialaccountsbyprogramactivityobjectclass',
            name='disaster_emergency_fund',
        )
    ]
