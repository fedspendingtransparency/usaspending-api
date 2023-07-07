from django.db import migrations
from usaspending_api.awards.models.transaction_fabs import vw_transaction_fabs_sql


class Migration(migrations.Migration):

    dependencies = [("awards", "0102_transaction_fpds_view_add_current_cd_cols")]

    operations = [
        migrations.RunSQL(
            sql=f"DROP VIEW IF EXISTS vw_transaction_fabs; {vw_transaction_fabs_sql}",
            reverse_sql="DROP VIEW IF EXISTS vw_transaction_fabs",
            # Without this, Django will try to actually change the old table names in another migration
            # This says we've already done it.
            state_operations=[
                migrations.AlterModelTable(
                    name="transactionfabs",
                    table="vw_transaction_fabs",
                )
            ],
        ),
        migrations.AlterModelOptions(
            name="transactionfabs",
            options={"managed": False},
        ),
    ]
