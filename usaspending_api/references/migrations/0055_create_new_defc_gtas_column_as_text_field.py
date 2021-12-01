# Manually created to handle zero downtime with SQL views while changing DEFC column type
# JIRA Ticket: DEV-7953

from django.db import migrations, models
from django.db.models import F


def copy_def_code(apps, _):
    """
        Used in the migration below to copy over the disaster_emergecny_fund_code column below so that
        it can be converted to a Foreign Key. This approach minimizes any downtime.
    """
    DisasterEmergencyFundCode = apps.get_model("references", "DisasterEmergencyFundCode")
    DisasterEmergencyFundCode.objects.all().update(code_temp=F("code"))


class Migration(migrations.Migration):

    dependencies = [
        ('references', '0054_auto_20210923_2201'),
    ]

    operations = [
        # The different in field_name and db_column name here are to allow query paths
        # to still function while Views are recreated
        migrations.AlterField(
            model_name='disasteremergencyfundcode',
            name='code',
            field=models.CharField(max_length=2, primary_key=True, serialize=False, db_column='code_old'),
        ),
        migrations.AddField(
            model_name='disasteremergencyfundcode',
            name='code_temp',
            field=models.TextField(null=True, db_index=False, db_column='code'),
        ),
        migrations.RunPython(copy_def_code, reverse_code=migrations.RunPython.noop),
        migrations.AddField(
            model_name='gtassf133balances',
            name='disaster_emergency_fund_temp',
            field=models.TextField(null=True, db_column='disaster_emergency_fund_code_temp')
        ),
    ]
