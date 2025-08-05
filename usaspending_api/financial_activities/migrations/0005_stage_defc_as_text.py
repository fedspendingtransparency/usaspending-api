# Manually created to handle zero downtime with SQL views while changing DEFC column type
# JIRA Ticket: DEV-7953

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('financial_activities', '0004_remove_financialaccountsbyprogramactivityobjectclass_final_of_fy'),
        ('references', '0055_create_new_defc_gtas_column_as_text_field'),
    ]

    operations = [
        migrations.AddField(
            model_name='financialaccountsbyprogramactivityobjectclass',
            name='disaster_emergency_fund_temp',
            field=models.TextField(null=True, db_column='disaster_emergency_fund_code_temp')
        )
    ]
