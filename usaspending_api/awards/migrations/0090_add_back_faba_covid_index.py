# Manually created to restore the FABA DB Index used to help COVID Profile Page queries
# JIRA Ticket: DEV-8486

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('awards', '0089_auto_20211019_1755'),
    ]

    operations = [
        migrations.RunSQL(
            sql=(
                "CREATE INDEX IF NOT EXISTS faba_subid_awardkey_sums_idx ON financial_accounts_by_awards ("
                "submission_id, distinct_award_key, piid, transaction_obligated_amount, "
                "gross_outlay_amount_by_award_cpe, ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe, "
                "ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe) "
                "WHERE disaster_emergency_fund_code IN ('L', 'M', 'N', 'O', 'P', 'U', 'V');"
            ),
            reverse_sql="DROP INDEX IF EXISTS faba_subid_awardkey_sums_idx;",
            state_operations=[
                migrations.AddIndex(
                    model_name='financialaccountsbyawards',
                    index=models.Index(
                        condition=models.Q(disaster_emergency_fund__in=["L", "M", "N", "O", "P", "U", "V"]),
                        fields=[
                            "submission",
                            "distinct_award_key",
                            "piid",
                            "transaction_obligated_amount",
                            "gross_outlay_amount_by_award_cpe",
                            "ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe",
                            "ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe"
                        ],
                        name="faba_subid_awardkey_sums_idx"
                    ),
                ),
            ]
        ),
    ]
