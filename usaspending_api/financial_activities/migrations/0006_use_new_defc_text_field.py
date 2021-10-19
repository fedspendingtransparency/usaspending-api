# Manually created to handle zero downtime with SQL views while changing DEFC column type
# JIRA Ticket: DEV-7953

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('financial_activities', '0005_stage_defc_as_text'),
    ]

    operations = [
        migrations.AlterField(
            model_name='financialaccountsbyprogramactivityobjectclass',
            name='disaster_emergency_fund',
            field=models.ForeignKey(
                blank=True,
                db_column='disaster_emergency_fund_code_old',
                null=True,
                on_delete=models.deletion.DO_NOTHING,
                to='references.DisasterEmergencyFundCode'
            ),
        ),
        migrations.RenameField(
            model_name='financialaccountsbyprogramactivityobjectclass',
            old_name='disaster_emergency_fund',
            new_name='disaster_emergency_fund_old'
        ),
        migrations.AlterField(
            model_name='financialaccountsbyprogramactivityobjectclass',
            name='disaster_emergency_fund_temp',
            field=models.TextField(null=True, db_column='disaster_emergency_fund_code')
        ),
        migrations.RenameField(
            model_name='financialaccountsbyprogramactivityobjectclass',
            old_name='disaster_emergency_fund_temp',
            new_name='disaster_emergency_fund'
        ),
    ]
