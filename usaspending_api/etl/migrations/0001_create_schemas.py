# Manual created to handle creation of different schemas

from django.db import migrations


class Migration(migrations.Migration):

    dependencies = [
        ("awards", "0092_transactionfpds_entity_data_source"),
        ("recipient", "0020_auto_20220406_1352"),
        ("search", "0007_transactionsearch_parent_uei"),
        ("transactions", "0008_sourceprocurementtransaction_entity_data_source")
    ]

    operations = [
        # -----
        # Create the different schemas
        # -----
        migrations.RunSQL(
            sql="CREATE SCHEMA IF NOT EXISTS raw;",
            reverse_sql="DROP SCHEMA raw;"
        ),
        migrations.RunSQL(
            sql="CREATE SCHEMA IF NOT EXISTS int;",
            reverse_sql="DROP SCHEMA int;"
        ),
        migrations.RunSQL(
            sql="CREATE SCHEMA IF NOT EXISTS temp;",
            reverse_sql="DROP SCHEMA temp;"
        ),
        migrations.RunSQL(
            sql="CREATE SCHEMA IF NOT EXISTS rpt;",
            reverse_sql="DROP SCHEMA rpt;"
        ),

        # -----
        # Move Bronze tables into the "raw" schema
        # -----
        migrations.RunSQL(
            sql="ALTER TABLE public.broker_subaward SET SCHEMA raw;",
            reverse_sql="ALTER TABLE raw.broker_subaward SET SCHEMA public;"
        ),

        # -----
        # Move Silver tables into the "int" schema
        # -----
        migrations.RunSQL(
            sql="ALTER TABLE public.subaward SET SCHEMA int;",
            reverse_sql="ALTER TABLE int.subaward SET SCHEMA public;"
        ),

        # -----
        # Move Materialized Views into the "temp" schema
        # -----

        # -----
        # Move Gold tables into the "rpt" schema
        # -----
        migrations.RunSQL(
            sql="ALTER TABLE IF EXISTS public.subaward_view SET SCHEMA rpt;",
            reverse_sql="ALTER TABLE IF EXISTS rpt.subaward_view SET SCHEMA public;"
        ),
    ]