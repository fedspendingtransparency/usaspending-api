# Manually created to handle zero downtime with SQL views while changing DEFC column type
# JIRA Ticket: DEV-7953

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('awards', '0086_stage_defc_as_text'),
    ]

    operations = [
        migrations.AlterIndexTogether(
            name='financialaccountsbyawards',
            index_together=set(),
        ),
        migrations.AlterField(
            model_name='financialaccountsbyawards',
            name='disaster_emergency_fund',
            field=models.ForeignKey(
                blank=True,
                db_column='disaster_emergency_fund_code_old',
                null=True,
                on_delete=models.deletion.DO_NOTHING,
                to='references.DisasterEmergencyFundCode'
            ),
        ),
        migrations.AlterField(
            model_name='financialaccountsbyawards',
            name='disaster_emergency_fund_temp',
            field=models.TextField(null=True, db_column='disaster_emergency_fund_code')
        ),
        migrations.AlterIndexTogether(
            name='financialaccountsbyawards',
            index_together={('disaster_emergency_fund_temp', 'submission', 'award', 'piid', 'fain', 'uri', 'parent_award_id', 'transaction_obligated_amount', 'gross_outlay_amount_by_award_cpe')},
        ),
    ]
