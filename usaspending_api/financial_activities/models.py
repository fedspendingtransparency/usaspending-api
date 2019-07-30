from django.db import models, connection

from usaspending_api.accounts.models import TreasuryAppropriationAccount
from usaspending_api.references.models import ObjectClass, RefProgramActivity
from usaspending_api.submissions.models import SubmissionAttributes
from usaspending_api.common.models import DataSourceTrackedModel


class FinancialAccountsByProgramActivityObjectClassManager(models.Manager):
    def get_queryset(self):
        """
        Get only records from the last submission per TAS per fiscal year.
        """

        return super(FinancialAccountsByProgramActivityObjectClassManager, self).get_queryset().filter(final_of_fy=True)


class FinancialAccountsByProgramActivityObjectClass(DataSourceTrackedModel):
    """ Model corresponding to Agency File B """

    financial_accounts_by_program_activity_object_class_id = models.AutoField(primary_key=True)
    program_activity = models.ForeignKey(RefProgramActivity, models.DO_NOTHING, null=True, db_index=True)
    submission = models.ForeignKey(SubmissionAttributes, models.CASCADE)
    object_class = models.ForeignKey(ObjectClass, models.DO_NOTHING, null=True, db_index=True)
    treasury_account = models.ForeignKey(
        TreasuryAppropriationAccount, models.CASCADE, related_name="program_balances", null=True
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
        UPDATE financial_accounts_by_program_activity_object_class
        SET final_of_fy = submission_id in
        ( SELECT DISTINCT ON
            (fabpaoc.treasury_account_id,
             FY(s.reporting_period_start))
          s.submission_id
          FROM submission_attributes s
          JOIN financial_accounts_by_program_activity_object_class fabpaoc
              ON (s.submission_id = fabpaoc.submission_id)
          ORDER BY fabpaoc.treasury_account_id,
                   FY(s.reporting_period_start),
                   s.reporting_period_start DESC)"""

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

    # TODO: is the self-joining SQL below do-able via the ORM?
    QUARTERLY_SQL = """
        SELECT
            current.financial_accounts_by_program_activity_object_class_id,
            sub.submission_id AS submission_id,
            current.treasury_account_id,
            current.object_class_id,
            current.program_activity_id,
            current.data_source,
            current.ussgl480100_undelivered_orders_obligations_unpaid_fyb - \
                COALESCE(previous.ussgl480100_undelivered_orders_obligations_unpaid_fyb, 0) \
                AS ussgl480100_undelivered_orders_obligations_unpaid_fyb,
            current.ussgl480100_undelivered_orders_obligations_unpaid_cpe - \
                COALESCE(previous.ussgl480100_undelivered_orders_obligations_unpaid_cpe, 0) \
                AS ussgl480100_undelivered_orders_obligations_unpaid_cpe,
            current.ussgl483100_undelivered_orders_oblig_transferred_unpaid_cpe - \
                COALESCE(previous.ussgl483100_undelivered_orders_oblig_transferred_unpaid_cpe, 0) \
                AS ussgl483100_undelivered_orders_oblig_transferred_unpaid_cpe,
            current.ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe - \
                COALESCE(previous.ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe, 0) \
                AS ussgl488100_upward_adjust_pri_undeliv_order_oblig_unpaid_cpe,
            current.ussgl490100_delivered_orders_obligations_unpaid_fyb - \
                COALESCE(previous.ussgl490100_delivered_orders_obligations_unpaid_fyb, 0) \
                AS ussgl490100_delivered_orders_obligations_unpaid_fyb,
            current.ussgl490100_delivered_orders_obligations_unpaid_cpe - \
                COALESCE(previous.ussgl490100_delivered_orders_obligations_unpaid_cpe, 0) \
                AS ussgl490100_delivered_orders_obligations_unpaid_cpe,
            current.ussgl493100_delivered_orders_oblig_transferred_unpaid_cpe - \
                COALESCE(previous.ussgl493100_delivered_orders_oblig_transferred_unpaid_cpe, 0) \
                AS ussgl493100_delivered_orders_oblig_transferred_unpaid_cpe,
            current.ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe - \
                COALESCE(previous.ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe, 0) \
                AS ussgl498100_upward_adjust_pri_deliv_orders_oblig_unpaid_cpe,
            current.ussgl480200_undelivered_orders_oblig_prepaid_advanced_fyb - \
                COALESCE(previous.ussgl480200_undelivered_orders_oblig_prepaid_advanced_fyb, 0) \
                AS ussgl480200_undelivered_orders_oblig_prepaid_advanced_fyb,
            current.ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe - \
                COALESCE(previous.ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe, 0) \
                AS ussgl480200_undelivered_orders_oblig_prepaid_advanced_cpe,
            current.ussgl483200_undeliv_orders_oblig_transferred_prepaid_adv_cpe - \
                COALESCE(previous.ussgl483200_undeliv_orders_oblig_transferred_prepaid_adv_cpe, 0) \
                AS ussgl483200_undeliv_orders_oblig_transferred_prepaid_adv_cpe,
            current.ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe - \
                COALESCE(previous.ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe, 0) \
                AS ussgl488200_up_adjust_pri_undeliv_order_oblig_ppaid_adv_cpe,
            current.ussgl490200_delivered_orders_obligations_paid_cpe - \
                COALESCE(previous.ussgl490200_delivered_orders_obligations_paid_cpe, 0) \
                AS ussgl490200_delivered_orders_obligations_paid_cpe,
            current.ussgl490800_authority_outlayed_not_yet_disbursed_fyb - \
                COALESCE(previous.ussgl490800_authority_outlayed_not_yet_disbursed_fyb, 0) \
                AS ussgl490800_authority_outlayed_not_yet_disbursed_fyb,
            current.ussgl490800_authority_outlayed_not_yet_disbursed_cpe - \
                COALESCE(previous.ussgl490800_authority_outlayed_not_yet_disbursed_cpe, 0) \
                AS ussgl490800_authority_outlayed_not_yet_disbursed_cpe,
            current.ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe - \
                COALESCE(previous.ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe, 0) \
                AS ussgl498200_upward_adjust_pri_deliv_orders_oblig_paid_cpe,
            current.obligations_undelivered_orders_unpaid_total_fyb - \
                COALESCE(previous.obligations_undelivered_orders_unpaid_total_fyb, 0) \
                AS obligations_undelivered_orders_unpaid_total_fyb,
            current.obligations_undelivered_orders_unpaid_total_cpe - \
                COALESCE(previous.obligations_undelivered_orders_unpaid_total_cpe, 0) \
                AS obligations_undelivered_orders_unpaid_total_cpe,
            current.obligations_delivered_orders_unpaid_total_fyb - \
                COALESCE(previous.obligations_delivered_orders_unpaid_total_fyb, 0) \
                AS obligations_delivered_orders_unpaid_total_fyb,
            current.obligations_delivered_orders_unpaid_total_cpe - \
                COALESCE(previous.obligations_delivered_orders_unpaid_total_cpe, 0) \
                AS obligations_delivered_orders_unpaid_total_cpe,
            current.gross_outlays_undelivered_orders_prepaid_total_fyb - \
                COALESCE(previous.gross_outlays_undelivered_orders_prepaid_total_fyb, 0) \
                AS gross_outlays_undelivered_orders_prepaid_total_fyb,
            current.gross_outlays_undelivered_orders_prepaid_total_cpe - \
                COALESCE(previous.gross_outlays_undelivered_orders_prepaid_total_cpe, 0) \
                AS gross_outlays_undelivered_orders_prepaid_total_cpe,
            current.gross_outlays_delivered_orders_paid_total_fyb - \
                COALESCE(previous.gross_outlays_delivered_orders_paid_total_fyb, 0) \
                AS gross_outlays_delivered_orders_paid_total_fyb,
            current.gross_outlays_delivered_orders_paid_total_cpe - \
                COALESCE(previous.gross_outlays_delivered_orders_paid_total_cpe, 0) \
                AS gross_outlays_delivered_orders_paid_total_cpe,
            current.gross_outlay_amount_by_program_object_class_fyb - \
                COALESCE(previous.gross_outlay_amount_by_program_object_class_fyb, 0) \
                AS gross_outlay_amount_by_program_object_class_fyb,
            current.gross_outlay_amount_by_program_object_class_cpe - \
                COALESCE(previous.gross_outlay_amount_by_program_object_class_cpe, 0) \
                AS gross_outlay_amount_by_program_object_class_cpe,
            current.obligations_incurred_by_program_object_class_cpe - \
                COALESCE(previous.obligations_incurred_by_program_object_class_cpe, 0) \
                AS obligations_incurred_by_program_object_class_cpe,
            current.ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe - \
                COALESCE(previous.ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe, 0) \
                AS ussgl487100_down_adj_pri_unpaid_undel_orders_oblig_recov_cpe,
            current.ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe - \
                COALESCE(previous.ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe, 0) \
                AS ussgl497100_down_adj_pri_unpaid_deliv_orders_oblig_recov_cpe,
            current.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe - \
                COALESCE(previous.ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe, 0) \
                AS ussgl487200_down_adj_pri_ppaid_undel_orders_oblig_refund_cpe,
            current.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe - \
                COALESCE(previous.ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe, 0) \
                AS ussgl497200_down_adj_pri_paid_deliv_orders_oblig_refund_cpe,
            current.deobligations_recoveries_refund_pri_program_object_class_cpe - \
                COALESCE(previous.deobligations_recoveries_refund_pri_program_object_class_cpe, 0) \
                AS deobligations_recoveries_refund_pri_program_object_class_cpe
        FROM
            financial_accounts_by_program_activity_object_class AS current
            JOIN submission_attributes AS sub
            ON current.submission_id = sub.submission_id
            LEFT JOIN financial_accounts_by_program_activity_object_class AS previous
            ON current.treasury_account_id = previous.treasury_account_id
            AND current.object_class_id = previous.object_class_id
            AND current.program_activity_id = previous.program_activity_id
            AND previous.submission_id = sub.previous_submission_id
    """

    @classmethod
    def get_quarterly_numbers(cls, current_submission_id=None):
        """
        Return a RawQuerySet of quarterly financial numbers by TAS,
        object class, and program activity.

        Because we receive financial data as YTD aggregates (i.e., Q3 numbers
        represent financial activity in Q1, Q2, and Q3), we subtract previously
        reported FY numbers when calculating discrete quarters.

        For example, consider a Q3 submission in fiscal year 2017. Using
        total_outlays (a simplified field name) as an example, we would get
        discrete Q3 total_outlays by taking total_outlays as reported in
        the Q3 submission and subtracting the total_outlays that were reported
        in the Q2 submission (or the most recent prior-to-Q3 submission we
        have on record for that agency in the current fiscal year).

        If there is no previous submission matching for an agency in the
        current fiscal year (for example, Q1 submission), quarterly numbers
        are the same as the submission's YTD numbers). The COALESCE function
        in the SQL above is what handles this scenario.

        Args:
            current_submission_id: the submission to retrieve quarterly data for
                (if None, return quarterly data for all submisisons)

        Returns:
            A RawQuerySet of FinancialAccountsByProgramActivityObjectClass objects
        """
        if current_submission_id is None:
            return FinancialAccountsByProgramActivityObjectClass.objects.raw(cls.QUARTERLY_SQL)
        else:
            sql = cls.QUARTERLY_SQL + " WHERE current.submission_id = %s"
            return FinancialAccountsByProgramActivityObjectClass.objects.raw(sql, [current_submission_id])


class TasProgramActivityObjectClassQuarterly(DataSourceTrackedModel):
    """
    Represents quarterly financial amounts by tas, program activity, and
    object class for each DATA Act broker submission.
    Each submission provides a snapshot of the most
    recent numbers for that fiscal year. Thus, to populate this model, we
    subtract previous quarters' balances from the balances of the current
    submission.

    Note: this model name isn't consistent with
    FinancialAccountsByProgramActivityObjectClass, but hopefull we'll change
    that to something easier to manage in the near future.
    """

    treasury_account = models.ForeignKey(TreasuryAppropriationAccount, models.CASCADE, null=True)
    program_activity = models.ForeignKey(RefProgramActivity, models.DO_NOTHING, null=True)
    object_class = models.ForeignKey(ObjectClass, models.DO_NOTHING, null=True)
    submission = models.ForeignKey(SubmissionAttributes, models.CASCADE)
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
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)

    class Meta:
        managed = True
        db_table = "tas_program_activity_object_class_quarterly"
        unique_together = ("treasury_account", "program_activity", "object_class", "submission")

    @classmethod
    def insert_quarterly_numbers(cls, submission_id=None):
        """
        Bulk insert quarterly finanical numbers by tas, program activity, and
        object class.
        """
        field_list = [field.column for field in TasProgramActivityObjectClassQuarterly._meta.get_fields()]

        # delete existing quarterly data
        if submission_id is None:
            TasProgramActivityObjectClassQuarterly.objects.all().delete()
        else:
            TasProgramActivityObjectClassQuarterly.objects.filter(submission_id=submission_id).delete()

        # retrieve RawQuerySet of quarterly breakouts
        qtr_records = FinancialAccountsByProgramActivityObjectClass.get_quarterly_numbers(submission_id)
        qtr_list = []

        # for each record in the RawQuerySet, create a corresponding
        # TasProgramActivityObjectClassQuarterly object and save it to a list
        # for subsequent bulk insert
        # TODO: maybe we don't want this entire list in memory?
        for rec in qtr_records:

            # remove any fields in the qtr_record RawQuerySet object so that we
            # can create its TasProgramActivityObjectClassQuarterly counterpart
            # more easily. it's a bit hacky, but avoids the need to explicitly
            # set dozens of attributes when creating TasProgramActivityObjectClassQuarterly
            rec_dict = rec.__dict__
            rec_dict = {key: rec_dict[key] for key in rec_dict if key in field_list}
            qtr_list.append(TasProgramActivityObjectClassQuarterly(**rec_dict))
        TasProgramActivityObjectClassQuarterly.objects.bulk_create(qtr_list)

    @classmethod
    def refresh_downstream_quarterly_numbers(cls, submission):
        """
        Recalculate quarterly financial numbers for any downstream submissions

        Use when the given submission has been re-loaded, so that values
        calculated based on changes in cumulative numbers may need
        recaclulation.

        "Downstream" means for the following quarter and, in turn, its
        following quarters (recursively) through the end of the year.
        """

        for downstream_submission in SubmissionAttributes.objects.filter(previous_submission=submission).all():
            cls.objects.filter(submission=downstream_submission).delete()
            cls.insert_quarterly_numbers(submission_id=downstream_submission.submission_id)
            cls.refresh_downstream_quarterly_numbers(downstream_submission)
