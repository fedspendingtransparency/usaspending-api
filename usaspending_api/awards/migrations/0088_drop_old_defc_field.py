# Manually created to handle zero downtime with SQL views while changing DEFC column type
# JIRA Ticket: DEV-7953

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('awards', '0087_use_new_defc_text_field'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='financialaccountsbyawards',
            name='disaster_emergency_fund',
        ),
    ]
