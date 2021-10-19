# Manually created to handle zero downtime with SQL views while changing DEFC column type
# JIRA Ticket: DEV-7953

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('awards', '0085_auto_20210929_2219'),
        ('references', '0055_create_new_defc_gtas_column_as_text_field'),
    ]

    operations = [
        migrations.AddField(
            model_name='financialaccountsbyawards',
            name='disaster_emergency_fund_temp',
            field=models.TextField(null=True, db_column='disaster_emergency_fund_code_temp')
        ),
    ]
