from django.db import migrations
from usaspending_api.awards.models.transaction_fpds import vw_transaction_fpds_sql


class Migration(migrations.Migration):

    dependencies = [
        ("awards", "0101_drop_pg_silver_int_tx_awd_tables"),
        ("search", "0032_as_ts_cur_congressional_codes"),
    ]

    operations = [
        migrations.RunSQL(
            sql=f"DROP VIEW IF EXISTS vw_transaction_fpds; {vw_transaction_fpds_sql}",
            reverse_sql="DROP VIEW IF EXISTS vw_transaction_fpds",
            # Without this, Django will try to actually change the old table names in another migration
            # This says we've already done it.
            state_operations=[
                migrations.AlterModelTable(
                    name="transactionfpds",
                    table="vw_transaction_fpds",
                )
            ],
        ),
        migrations.AlterModelOptions(
            name="transactionfpds",
            options={"managed": False},
        ),
    ]
