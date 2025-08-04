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
            sql="ALTER TABLE IF EXISTS public.broker_subaward SET SCHEMA raw;",
            reverse_sql="ALTER TABLE raw.broker_subaward SET SCHEMA public;"
        ),
        migrations.RunSQL(
            sql="ALTER TABLE public.duns SET SCHEMA raw;",
            reverse_sql="ALTER TABLE raw.duns SET SCHEMA public;"
        ),
        migrations.RunSQL(
            sql="ALTER TABLE public.source_assistance_transaction SET SCHEMA raw;",
            reverse_sql="ALTER TABLE raw.source_assistance_transaction SET SCHEMA public;"
        ),
        migrations.RunSQL(
            sql="ALTER TABLE public.source_procurement_transaction SET SCHEMA raw;",
            reverse_sql="ALTER TABLE raw.source_procurement_transaction SET SCHEMA public;"
        ),

        # -----
        # Move Silver tables into the "int" schema
        # -----
        migrations.RunSQL(
            sql="ALTER TABLE IF EXISTS public.subaward SET SCHEMA int;",
            reverse_sql="ALTER TABLE int.subaward SET SCHEMA public;"
        ),
        migrations.RunSQL(
            sql="ALTER TABLE public.transaction_normalized SET SCHEMA int;",
            reverse_sql="ALTER TABLE int.transaction_normalized SET SCHEMA public;"
        ),
        migrations.RunSQL(
            sql="ALTER TABLE public.transaction_fabs SET SCHEMA int;",
            reverse_sql="ALTER TABLE int.transaction_fabs SET SCHEMA public;"
        ),
        migrations.RunSQL(
            sql="ALTER TABLE public.transaction_fpds SET SCHEMA int;",
            reverse_sql="ALTER TABLE int.transaction_fpds SET SCHEMA public;"
        ),
        migrations.RunSQL(
            sql="ALTER TABLE public.transaction_delta SET SCHEMA int;",
            reverse_sql="ALTER TABLE int.transaction_delta SET SCHEMA public;"
        ),

        # -----
        # Move Materialized Views into the "temp" schema
        # -----

        # TransactionSearch Materialized Views
        migrations.RunSQL(
            sql="ALTER TABLE IF EXISTS public.transaction_search_0 SET SCHEMA temp;",
            reverse_sql="ALTER TABLE IF EXISTS temp.transaction_search_0 SET SCHEMA public;"
        ),
        migrations.RunSQL(
            sql="ALTER TABLE IF EXISTS public.transaction_search_1 SET SCHEMA temp;",
            reverse_sql="ALTER TABLE IF EXISTS temp.transaction_search_1 SET SCHEMA public;"
        ),
        migrations.RunSQL(
            sql="ALTER TABLE IF EXISTS public.transaction_search_2 SET SCHEMA temp;",
            reverse_sql="ALTER TABLE IF EXISTS temp.transaction_search_2 SET SCHEMA public;"
        ),
        migrations.RunSQL(
            sql="ALTER TABLE IF EXISTS public.transaction_search_3 SET SCHEMA temp;",
            reverse_sql="ALTER TABLE IF EXISTS temp.transaction_search_3 SET SCHEMA public;"
        ),
        migrations.RunSQL(
            sql="ALTER TABLE IF EXISTS public.transaction_search_4 SET SCHEMA temp;",
            reverse_sql="ALTER TABLE IF EXISTS temp.transaction_search_4 SET SCHEMA public;"
        ),
        migrations.RunSQL(
            sql="ALTER TABLE IF EXISTS public.transaction_search_5 SET SCHEMA temp;",
            reverse_sql="ALTER TABLE IF EXISTS temp.transaction_search_5 SET SCHEMA public;"
        ),
        migrations.RunSQL(
            sql="ALTER TABLE IF EXISTS public.transaction_search_6 SET SCHEMA temp;",
            reverse_sql="ALTER TABLE IF EXISTS temp.transaction_search_6 SET SCHEMA public;"
        ),
        migrations.RunSQL(
            sql="ALTER TABLE IF EXISTS public.transaction_search_7 SET SCHEMA temp;",
            reverse_sql="ALTER TABLE IF EXISTS temp.transaction_search_7 SET SCHEMA public;"
        ),
        migrations.RunSQL(
            sql="ALTER TABLE IF EXISTS public.transaction_search_8 SET SCHEMA temp;",
            reverse_sql="ALTER TABLE IF EXISTS temp.transaction_search_8 SET SCHEMA public;"
        ),
        migrations.RunSQL(
            sql="ALTER TABLE IF EXISTS public.transaction_search_9 SET SCHEMA temp;",
            reverse_sql="ALTER TABLE IF EXISTS temp.transaction_search_9 SET SCHEMA public;"
        ),

        # AwardSearch Materialized Views
        migrations.RunSQL(
            sql="ALTER TABLE IF EXISTS public.mv_contract_award_search SET SCHEMA temp;",
            reverse_sql="ALTER TABLE IF EXISTS temp.mv_contract_award_search SET SCHEMA public;"
        ),
        migrations.RunSQL(
            sql="ALTER TABLE IF EXISTS public.mv_idv_award_search SET SCHEMA temp;",
            reverse_sql="ALTER TABLE IF EXISTS temp.mv_idv_award_search SET SCHEMA public;"
        ),
        migrations.RunSQL(
            sql="ALTER TABLE IF EXISTS public.mv_loan_award_search SET SCHEMA temp;",
            reverse_sql="ALTER TABLE IF EXISTS temp.mv_loan_award_search SET SCHEMA public;"
        ),
        migrations.RunSQL(
            sql="ALTER TABLE IF EXISTS public.mv_grant_award_search SET SCHEMA temp;",
            reverse_sql="ALTER TABLE IF EXISTS temp.mv_grant_award_search SET SCHEMA public;"
        ),
        migrations.RunSQL(
            sql="ALTER TABLE IF EXISTS public.mv_directpayment_award_search SET SCHEMA temp;",
            reverse_sql="ALTER TABLE IF EXISTS temp.mv_directpayment_award_search SET SCHEMA public;"
        ),
        migrations.RunSQL(
            sql="ALTER TABLE IF EXISTS public.mv_other_award_search SET SCHEMA temp;",
            reverse_sql="ALTER TABLE IF EXISTS temp.mv_other_award_search SET SCHEMA public;"
        ),
        migrations.RunSQL(
            sql="ALTER TABLE IF EXISTS public.mv_pre2008_award_search SET SCHEMA temp;",
            reverse_sql="ALTER TABLE IF EXISTS temp.mv_pre2008_award_search SET SCHEMA public;"
        ),

        # -----
        # Move Gold tables into the "rpt" schema
        # -----
        migrations.RunSQL(
            sql="ALTER TABLE IF EXISTS public.subaward_view SET SCHEMA rpt;",
            reverse_sql="ALTER TABLE IF EXISTS rpt.subaward_view SET SCHEMA public;"
        ),
        migrations.RunSQL(
            sql="ALTER TABLE public.recipient_lookup SET SCHEMA rpt;",
            reverse_sql="ALTER TABLE rpt.recipient_lookup SET SCHEMA public;"
        ),
        migrations.RunSQL(
            sql="ALTER TABLE public.recipient_profile SET SCHEMA rpt;",
            reverse_sql="ALTER TABLE rpt.recipient_profile SET SCHEMA public;"
        ),
        migrations.RunSQL(
            sql="ALTER TABLE public.transaction_search SET SCHEMA rpt;",
            reverse_sql="ALTER TABLE rpt.transaction_search SET SCHEMA public;"
        ),
        migrations.RunSQL(
            sql="ALTER TABLE public.awards SET SCHEMA rpt;",
            reverse_sql="ALTER TABLE rpt.awards SET SCHEMA public;"
        ),
        migrations.RunSQL(
            sql="ALTER TABLE public.parent_award SET SCHEMA rpt;",
            reverse_sql="ALTER TABLE rpt.parent_award SET SCHEMA public;"
        ),
        migrations.RunSQL(
            sql="ALTER TABLE IF EXISTS public.vw_award_search SET SCHEMA rpt;",
            reverse_sql="ALTER TABLE IF EXISTS rpt.vw_award_search SET SCHEMA public;"
        ),
    ]
