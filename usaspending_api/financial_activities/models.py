from django.db import models, connection
from usaspending_api.accounts.models import TreasuryAppropriationAccount
from usaspending_api.references.models import ObjectClass, RefProgramActivity
from usaspending_api.submissions.models import SubmissionAttributes
from usaspending_api.common.models import DataSourceTrackedModel


class AbstractFinancialAccountsByProgramActivityObjectClass(DataSourceTrackedModel):
    financial_accounts_by_program_activity_object_class_id = models.AutoField(primary_key=True)
    program_activity = models.ForeignKey(RefProgramActivity, models.DO_NOTHING, null=True, db_index=True)
    submission = models.ForeignKey(SubmissionAttributes, models.CASCADE)
    object_class = models.ForeignKey(ObjectClass, models.DO_NOTHING, null=True, db_index=True)
    treasury_account = models.ForeignKey(TreasuryAppropriationAccount, models.CASCADE, null=True)
    disaster_emergency_fund = models.ForeignKey(
        "references.DisasterEmergencyFundCode",
        models.DO_NOTHING,
        blank=True,
        null=True,
        db_index=True,
        db_column="disaster_emergency_fund_code",
    )
    ussgl480100_undelivered_orders_obligations_unpaid_fyb = models.DecimalField(max_digits=23, decimal_places=2)
    ussgl480100_undelivered_orders_obligations_unpaid_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    ussgl483100_undelivered_orders_oblig_transferred_unpaid_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    ussgl490100_delivered_orders_obligations_unpaid_fyb = models.DecimalField(max_digits=23, decimal_places=2)
    ussgl490100_delivered_orders_obligations_unpaid_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    ussgl493100_delivered_orders_oblig_transferred_unpaid_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    ussgl480200_undelivered_orders_oblig_prepaid_advanced_fyb = models.DecimalField(max_digits=23, decimal_places=2)
    ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    ussgl483200_undeliv_orders_oblig_transferred_prepaid_adv_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    ussgl490200_delivered_orders_obligations_paid_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    ussgl490800_authority_outlayed_not_yet_disbursed_fyb = models.DecimalField(max_digits=23, decimal_places=2)
    ussgl490800_authority_outlayed_not_yet_disbursed_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    obligations_undelivered_orders_unpaid_total_fyb = models.DecimalField(max_digits=23, decimal_places=2)
    obligations_undelivered_orders_unpaid_total_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    obligations_delivered_orders_unpaid_total_fyb = models.DecimalField(max_digits=23, decimal_places=2)
    obligations_delivered_orders_unpaid_total_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    gross_outlays_undelivered_orders_prepaid_total_fyb = models.DecimalField(max_digits=23, decimal_places=2)
    gross_outlays_undelivered_orders_prepaid_total_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    gross_outlays_delivered_orders_paid_total_fyb = models.DecimalField(max_digits=23, decimal_places=2)
    gross_outlays_delivered_orders_paid_total_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    gross_outlay_amount_by_program_object_class_fyb = models.DecimalField(max_digits=23, decimal_places=2)
    gross_outlay_amount_by_program_object_class_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    obligations_incurred_by_program_object_class_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    deobligations_recoveries_refund_pri_program_object_class_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    drv_obligations_incurred_by_program_object_class = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True
    )
    drv_obligations_undelivered_orders_unpaid = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True
    )
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)
    last_modified_date = models.DateField(blank=True, null=True)
    certified_date = models.DateField(blank=True, null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)
    final_of_fy = models.BooleanField(blank=False, null=False, default=False, db_index=True)

    class Meta:
        abstract = True


class FinancialAccountsByProgramActivityObjectClassManager(models.Manager):
    def get_queryset(self):
        """
        Get only records from the last submission per TAS per fiscal year.
        """

        return super(FinancialAccountsByProgramActivityObjectClassManager, self).get_queryset().filter(final_of_fy=True)


class FinancialAccountsByProgramActivityObjectClass(AbstractFinancialAccountsByProgramActivityObjectClass):
    """ Model corresponding to Agency File B """

    # Overriding to set the "related_name"
    treasury_account = models.ForeignKey(
        TreasuryAppropriationAccount, models.CASCADE, related_name="program_balances", null=True
    )

    class Meta:
        managed = True
        db_table = "financial_accounts_by_program_activity_object_class"

    objects = models.Manager()
    final_objects = FinancialAccountsByProgramActivityObjectClassManager()

    """
    Note: The FINAL_OF_FY_SQL will be used despite UPDATED_FINAL_OF_FY_SQL being more accurate.
          Reasons for this decision are twofold
            1. It is significantly more performant than the more accurate SQL
            2. The endpoints which use this field are V1 which don't require the effort for accuracy and performance.
    """
    FINAL_OF_FY_SQL = """
        with submission_ids as (
            select distinct submission_id from (
                select distinct on (f.treasury_account_id, s.reporting_fiscal_year)
                    s.submission_id
                from
                    submission_attributes s
                    inner join financial_accounts_by_program_activity_object_class f on s.submission_id = f.submission_id
                order by
                    f.treasury_account_id,
                    s.reporting_fiscal_year,
                    s.reporting_period_end desc
            ) t
        )
        update  financial_accounts_by_program_activity_object_class f
        set     final_of_fy = (submission_id in (select submission_id from submission_ids))
        where   final_of_fy != (submission_id in (select submission_id from submission_ids))
    """

    UPDATED_FINAL_OF_FY_SQL = """
        UPDATE financial_accounts_by_program_activity_object_class
        SET final_of_fy = (treasury_account_id, program_activity_id, object_class_id, submission_id) in
        ( SELECT DISTINCT ON
            (fabpaoc.treasury_account_id,
             fabpaoc.program_activity_id,
             fabpaoc.object_class_id,
             FY(s.reporting_period_start))
          fabpaoc.treasury_account_id,
          fabpaoc.program_activity_id,
          fabpaoc.object_class_id,
          s.submission_id
          FROM submission_attributes s
          JOIN financial_accounts_by_program_activity_object_class fabpaoc
              ON (s.submission_id = fabpaoc.submission_id)
          ORDER BY fabpaoc.treasury_account_id,
                   fabpaoc.program_activity_id,
                   fabpaoc.object_class_id,
                   FY(s.reporting_period_start),
                   s.reporting_period_start DESC)"""

    @classmethod
    def populate_final_of_fy(cls):
        with connection.cursor() as cursor:
            cursor.execute(cls.FINAL_OF_FY_SQL)
