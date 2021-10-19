# Manually created to handle zero downtime with SQL views while changing DEFC column type
# JIRA Ticket: DEV-7953

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('awards', '0088_drop_old_defc_field'),
        ('financial_activities', '0007_drop_old_defc_field'),
        ('references', '0056_use_new_defc_text_field')
    ]

    operations = [
        migrations.RemoveField(
            model_name='gtassf133balances',
            name='disaster_emergency_fund_old',
        ),
        migrations.RemoveField(
            model_name='disasteremergencyfundcode',
            name='code_old',
        ),
        migrations.AlterField(
            model_name='disasteremergencyfundcode',
            name='code',
            field=models.TextField(primary_key=True, serialize=False),
        ),
        migrations.AlterField(
            model_name='gtassf133balances',
            name='disaster_emergency_fund',
            field=models.ForeignKey(
                blank=True,
                db_column='disaster_emergency_fund_code',
                null=True,
                on_delete=models.deletion.DO_NOTHING,
                to='references.DisasterEmergencyFundCode'
            ),
        ),
    ]
