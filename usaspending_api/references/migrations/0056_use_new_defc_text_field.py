# Manually created to handle zero downtime with SQL views while changing DEFC column type
# JIRA Ticket: DEV-7953

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('references', '0055_create_new_defc_gtas_column_as_text_field'),
    ]

    operations = [
        migrations.AlterUniqueTogether(
            name='gtassf133balances',
            unique_together=set()
        ),
        migrations.AlterField(
            model_name='gtassf133balances',
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
            model_name='gtassf133balances',
            old_name='disaster_emergency_fund',
            new_name='disaster_emergency_fund_old'
        ),
        migrations.AlterField(
            model_name='gtassf133balances',
            name='disaster_emergency_fund_temp',
            field=models.TextField(null=True, db_column='disaster_emergency_fund_code')
        ),
        migrations.RenameField(
            model_name='gtassf133balances',
            old_name='disaster_emergency_fund_temp',
            new_name='disaster_emergency_fund'
        ),
        migrations.AlterUniqueTogether(
            name='gtassf133balances',
            unique_together={('fiscal_year', 'fiscal_period', 'disaster_emergency_fund', 'tas_rendering_label')},
        ),
    ]
