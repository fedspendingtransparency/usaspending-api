from collections import defaultdict
from decimal import Decimal
from django.db import models, connection
from usaspending_api.common.helpers.generic_helper import fy
from usaspending_api.submissions.models import SubmissionAttributes
from usaspending_api.references.models import ToptierAgency
from usaspending_api.common.models import DataSourceTrackedModel


class FederalAccount(models.Model):
    """
    Represents a single federal account. A federal account encompasses multiple Treasury Account Symbols (TAS),
    represented by: model:`accounts.TreasuryAppropriationAccount`.
    """

    agency_identifier = models.TextField(db_index=True)
    main_account_code = models.TextField(db_index=True)
    account_title = models.TextField()
    federal_account_code = models.TextField(null=True)  # agency_identifier + '-' + main_account_code

    class Meta:
        managed = True
        db_table = "federal_account"
        unique_together = ("agency_identifier", "main_account_code")

    def save(self, *args, **kwargs):
        self.federal_account_code = self.agency_identifier + "-" + self.main_account_code
        super().save(*args, **kwargs)


class TreasuryAppropriationAccount(DataSourceTrackedModel):
    """Represents a single Treasury Account Symbol (TAS)."""

    treasury_account_identifier = models.AutoField(primary_key=True)
    federal_account = models.ForeignKey("FederalAccount", models.DO_NOTHING, null=True)
    tas_rendering_label = models.TextField(blank=True, null=True)
    allocation_transfer_agency_id = models.TextField(blank=True, null=True)
    awarding_toptier_agency = models.ForeignKey(
        "references.ToptierAgency",
        models.DO_NOTHING,
        null=True,
        related_name="tas_ata",
        help_text="The toptier agency object associated with the ATA",
    )
    # todo: update the agency details to match FederalAccounts. Is there a way that we can retain the text-based agency
    # TAS components (since those are attributes of TAS while still having a convenient FK that links to our agency
    # tables using Django's default fk naming standard? Something like agency_identifier for the 3 digit TAS
    # component and agency_id for the FK?)
    agency_id = models.TextField()
    funding_toptier_agency = models.ForeignKey(
        "references.ToptierAgency",
        models.DO_NOTHING,
        null=True,
        related_name="tas_aid",
        help_text="The toptier agency object associated with the AID",
    )
    beginning_period_of_availability = models.TextField(blank=True, null=True)
    ending_period_of_availability = models.TextField(blank=True, null=True)
    availability_type_code = models.TextField(blank=True, null=True)
    availability_type_code_description = models.TextField(blank=True, null=True)
    main_account_code = models.TextField()
    sub_account_code = models.TextField()
    account_title = models.TextField(blank=True, null=True)
    reporting_agency_id = models.TextField(blank=True, null=True)
    reporting_agency_name = models.TextField(blank=True, null=True)
    budget_bureau_code = models.TextField(blank=True, null=True)
    budget_bureau_name = models.TextField(blank=True, null=True)
    fr_entity_code = models.TextField(blank=True, null=True)
    fr_entity_description = models.TextField(blank=True, null=True)
    budget_function_code = models.TextField(blank=True, null=True)
    budget_function_title = models.TextField(blank=True, null=True)
    budget_subfunction_code = models.TextField(blank=True, null=True)
    budget_subfunction_title = models.TextField(blank=True, null=True)
    drv_appropriation_availability_period_start_date = models.DateField(blank=True, null=True)
    drv_appropriation_availability_period_end_date = models.DateField(blank=True, null=True)
    drv_appropriation_account_expired_status = models.TextField(blank=True, null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)

    def update_agency_linkages(self):
        self.awarding_toptier_agency = (
            ToptierAgency.objects.filter(cgac_code=self.allocation_transfer_agency_id).order_by("fpds_code").first()
        )
        self.funding_toptier_agency = (
            ToptierAgency.objects.filter(cgac_code=self.agency_id).order_by("fpds_code").first()
        )

    @staticmethod
    def generate_tas_rendering_label(ata, aid, typecode, bpoa, epoa, mac, sub):
        tas_rendering_label = "-".join(filter(None, (ata, aid)))

        if typecode is not None and typecode != "":
            tas_rendering_label = "-".join(filter(None, (tas_rendering_label, typecode)))
        else:
            poa = "/".join(filter(None, (bpoa, epoa)))
            tas_rendering_label = "-".join(filter(None, (tas_rendering_label, poa)))

        tas_rendering_label = "-".join(filter(None, (tas_rendering_label, mac, sub)))

        return tas_rendering_label

    @property
    def program_activities(self):
        return [pb.program_activity for pb in self.program_balances.distinct("program_activity")]

    @property
    def future_object_classes(self):
        # TODO: Once FinancialAccountsByProgramActivityObjectClass.object_class has been fixed to point to ObjectClass
        # instead of RefObjectClassCode, this will work with:
        #   return [pb.object_class for pb in self.program_balances.distinct('object_class')]
        results = []
        return results

    @property
    def object_classes(self):
        return [pb.object_class for pb in self.program_balances.distinct("object_class")]

    @property
    def totals_object_class(self):
        results = []
        for object_class in self.object_classes:
            obligations = defaultdict(Decimal)
            outlays = defaultdict(Decimal)
            for pb in self.program_balances.filter(object_class=object_class):
                reporting_fiscal_year = fy(pb.submission.reporting_period_start)
                obligations[reporting_fiscal_year] += pb.obligations_incurred_by_program_object_class_cpe
                outlays[reporting_fiscal_year] += pb.gross_outlay_amount_by_program_object_class_cpe
            result = {
                "major_object_class_code": None,
                "major_object_class_name": None,  # TODO: enable once ObjectClass populated
                "object_class": object_class.object_class,  # TODO: remove
                "outlays": obligations,
                "obligations": outlays,
            }
            results.append(result)
        return results

    @property
    def totals_program_activity(self):
        results = []
        for pa in self.program_activities:
            obligations = defaultdict(Decimal)
            outlays = defaultdict(Decimal)
            for pb in self.program_balances.filter(program_activity=pa):
                reporting_fiscal_year = fy(pb.submission.reporting_period_start)
                # TODO: once it is present, use the reporting_fiscal_year directly
                obligations[reporting_fiscal_year] += pb.obligations_incurred_by_program_object_class_cpe
                outlays[reporting_fiscal_year] += pb.gross_outlay_amount_by_program_object_class_cpe
            result = {
                "id": pa.id,
                "program_activity_name": pa.program_activity_name,
                "program_activity_code": pa.program_activity_code,
                "obligations": obligations,
                "outlays": outlays,
            }
            results.append(result)
        return results

    @property
    def totals(self):
        outlays = defaultdict(Decimal)
        obligations = defaultdict(Decimal)
        budget_authority = defaultdict(Decimal)
        for ab in self.account_balances.all():
            fiscal_year = fy(ab.reporting_period_start)
            budget_authority[fiscal_year] += ab.budget_authority_appropriated_amount_cpe
            outlays[fiscal_year] += ab.gross_outlay_amount_by_tas_cpe
            obligations[fiscal_year] += ab.obligations_incurred_total_by_tas_cpe
        results = {
            "outgoing": {"outlays": outlays, "obligations": obligations, "budget_authority": budget_authority},
            "incoming": {},
        }
        return results

    class Meta:
        managed = True
        db_table = "treasury_appropriation_account"

    def __str__(self):
        return "%s" % (self.tas_rendering_label)


class AppropriationAccountBalancesManager(models.Manager):
    def get_queryset(self):
        """
        Get only records from the last submission per TAS per fiscal year.
        """

        return super(AppropriationAccountBalancesManager, self).get_queryset().filter(final_of_fy=True)


class AppropriationAccountBalances(DataSourceTrackedModel):
    """
    Represents Treasury Account Symbol (TAS) balances for each DATA Act
    broker submission. Each submission provides a snapshot of the most
    recent numbers for that fiscal year. In other words, the lastest
    submission for a fiscal year reflects the balances for the entire
    fiscal year.
    """

    appropriation_account_balances_id = models.AutoField(primary_key=True)
    treasury_account_identifier = models.ForeignKey(
        "TreasuryAppropriationAccount",
        models.CASCADE,
        db_column="treasury_account_identifier",
        related_name="account_balances",
    )
    submission = models.ForeignKey(SubmissionAttributes, models.CASCADE)
    budget_authority_unobligated_balance_brought_forward_fyb = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True
    )
    adjustments_to_unobligated_balance_brought_forward_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    budget_authority_appropriated_amount_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    borrowing_authority_amount_total_cpe = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    contract_authority_amount_total_cpe = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    spending_authority_from_offsetting_collections_amount_cpe = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True
    )
    other_budgetary_resources_amount_cpe = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    total_budgetary_resources_amount_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    gross_outlay_amount_by_tas_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    deobligations_recoveries_refunds_by_tas_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    unobligated_balance_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    status_of_budgetary_resources_total_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    obligations_incurred_total_by_tas_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    drv_appropriation_availability_period_start_date = models.DateField(blank=True, null=True)
    drv_appropriation_availability_period_end_date = models.DateField(blank=True, null=True)
    drv_appropriation_account_expired_status = models.TextField(blank=True, null=True)
    drv_obligations_unpaid_amount = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    drv_other_obligated_amount = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)
    last_modified_date = models.DateField(blank=True, null=True)
    certified_date = models.DateField(blank=True, null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)
    final_of_fy = models.BooleanField(blank=False, null=False, default=False, db_index=True)

    class Meta:
        managed = True
        db_table = "appropriation_account_balances"

    objects = models.Manager()
    final_objects = AppropriationAccountBalancesManager()

    FINAL_OF_FY_SQL = """
        WITH submission_and_tai AS (
            SELECT
                DISTINCT ON (aab.treasury_account_identifier, FY(s.reporting_period_start))
                aab.treasury_account_identifier,
                s.submission_id
            FROM submission_attributes s
            JOIN appropriation_account_balances aab
                  ON (s.submission_id = aab.submission_id)
            ORDER BY aab.treasury_account_identifier,
                       FY(s.reporting_period_start),
                       s.reporting_period_start DESC
        )
        UPDATE appropriation_account_balances aab
            SET final_of_fy = true
            FROM submission_and_tai sat
            WHERE aab.treasury_account_identifier = sat.treasury_account_identifier AND
            aab.submission_id = sat.submission_id"""

    @classmethod
    def populate_final_of_fy(cls):
        with connection.cursor() as cursor:
            cursor.execute("UPDATE appropriation_account_balances SET final_of_fy = false")
            cursor.execute(cls.FINAL_OF_FY_SQL)

    # TODO: is the self-joining SQL below do-able via the ORM?
    # note: appropriation_account_balances_id is included in the quarterly query because Django's
    # Manage.raw()/RawQuerySet require the underlying model's primary key
    QUARTERLY_SQL = """
        SELECT
            current.appropriation_account_balances_id,
            sub.submission_id,
            current.treasury_account_identifier,
            current.data_source,
            COALESCE(current.budget_authority_unobligated_balance_brought_forward_fyb, 0) -
                COALESCE(previous.budget_authority_unobligated_balance_brought_forward_fyb, 0)
                AS budget_authority_unobligated_balance_brought_forward_fyb,
            COALESCE(current.adjustments_to_unobligated_balance_brought_forward_cpe, 0) -
                COALESCE(previous.adjustments_to_unobligated_balance_brought_forward_cpe, 0)
                AS adjustments_to_unobligated_balance_brought_forward_cpe,
            COALESCE(current.budget_authority_appropriated_amount_cpe, 0) -
                COALESCE(previous.budget_authority_appropriated_amount_cpe, 0)
                AS budget_authority_appropriated_amount_cpe,
            COALESCE(current.borrowing_authority_amount_total_cpe, 0) -
                COALESCE(previous.borrowing_authority_amount_total_cpe, 0) AS borrowing_authority_amount_total_cpe,
            COALESCE(current.contract_authority_amount_total_cpe, 0) -
                COALESCE(previous.contract_authority_amount_total_cpe, 0) AS contract_authority_amount_total_cpe,
            COALESCE(current.spending_authority_from_offsetting_collections_amount_cpe, 0) -
                COALESCE(previous.spending_authority_from_offsetting_collections_amount_cpe, 0)
                AS spending_authority_from_offsetting_collections_amount_cpe,
            COALESCE(current.other_budgetary_resources_amount_cpe, 0) -
                COALESCE(previous.other_budgetary_resources_amount_cpe, 0) AS other_budgetary_resources_amount_cpe,
            COALESCE(current.total_budgetary_resources_amount_cpe, 0) -
                COALESCE(previous.total_budgetary_resources_amount_cpe, 0)
                AS total_budgetary_resources_amount_cpe,
            COALESCE(current.gross_outlay_amount_by_tas_cpe, 0) -
                COALESCE(previous.gross_outlay_amount_by_tas_cpe, 0) AS gross_outlay_amount_by_tas_cpe,
            COALESCE(current.deobligations_recoveries_refunds_by_tas_cpe, 0) -
                COALESCE(previous.deobligations_recoveries_refunds_by_tas_cpe, 0)
                AS deobligations_recoveries_refunds_by_tas_cpe,
            COALESCE(current.unobligated_balance_cpe, 0) - COALESCE(previous.unobligated_balance_cpe, 0)
                AS unobligated_balance_cpe,
            COALESCE(current.status_of_budgetary_resources_total_cpe, 0) -
                COALESCE(previous.status_of_budgetary_resources_total_cpe, 0)
                AS status_of_budgetary_resources_total_cpe,
            COALESCE(current.obligations_incurred_total_by_tas_cpe, 0) -
                COALESCE(previous.obligations_incurred_total_by_tas_cpe, 0) AS obligations_incurred_total_by_tas_cpe
        FROM
            appropriation_account_balances AS current
            JOIN submission_attributes AS sub
            ON current.submission_id = sub.submission_id
            LEFT JOIN appropriation_account_balances AS previous
            ON current.treasury_account_identifier = previous.treasury_account_identifier
            AND previous.submission_id = sub.previous_submission_id
    """

    @classmethod
    def get_quarterly_numbers(cls, current_submission_id=None):
        """
        Return a RawQuerySet of quarterly financial numbers by tas (aka treasury account symbol, aka appropriations
        account)

        Because we receive financial data as YTD aggregates (i.e., Q3 numbers represent financial activity in Q1, Q2,
        and Q3), we subtract previously reported FY numbers when calculating discrete quarters.

        For example, consider a Q3 submission in fiscal year 2017. Using total_outlays (a simplified field name) as an
        example, we would get discrete Q3 total_outlays by taking total_outlays as reported in the Q3 submission and
        subtracting the total_outlays that were reported in the Q2 submission (or the most recent prior-to-Q3 submission
        we have on record for that agency in the current fiscal year).

        If there is no previous submission matching for an agency in the current fiscal year (for example, Q1
        submission), quarterly numbers are the same as the submission's YTD numbers). The COALESCE function in the SQL
        above is what handles this scenario.

        Args:
            current_submission_id: the submission to retrieve quarterly data for
                (if None, return quarterly data for all submisisons)

        Returns:
            A RawQuerySet of AppropriationAccountBalances objects
        """
        if current_submission_id is None:
            return AppropriationAccountBalances.objects.raw(cls.QUARTERLY_SQL)
        else:
            sql = cls.QUARTERLY_SQL + " WHERE current.submission_id = %s"
            return AppropriationAccountBalances.objects.raw(sql, [current_submission_id])


class AppropriationAccountBalancesQuarterly(DataSourceTrackedModel):
    """
    Represents quarterly financial amounts by tas (aka, treasury account symbol, aka appropriation account).
    Each data broker submission provides a snapshot of the most recent numbers for that fiscal year. Thus, to populate
    this model, we subtract previous quarters' balances from the balances of the current submission.
    """

    treasury_account_identifier = models.ForeignKey("TreasuryAppropriationAccount", models.CASCADE)
    submission = models.ForeignKey(SubmissionAttributes, models.CASCADE)
    budget_authority_unobligated_balance_brought_forward_fyb = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True
    )
    adjustments_to_unobligated_balance_brought_forward_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    budget_authority_appropriated_amount_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    borrowing_authority_amount_total_cpe = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    contract_authority_amount_total_cpe = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    spending_authority_from_offsetting_collections_amount_cpe = models.DecimalField(
        max_digits=23, decimal_places=2, blank=True, null=True
    )
    other_budgetary_resources_amount_cpe = models.DecimalField(max_digits=23, decimal_places=2, blank=True, null=True)
    total_budgetary_resources_amount_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    gross_outlay_amount_by_tas_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    deobligations_recoveries_refunds_by_tas_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    unobligated_balance_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    status_of_budgetary_resources_total_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    obligations_incurred_total_by_tas_cpe = models.DecimalField(max_digits=23, decimal_places=2)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)

    class Meta:
        managed = True
        db_table = "appropriation_account_balances_quarterly"
        # TODO: find out why this unique index causes constraint violations w/ our data
        # unique_together = ('treasury_account_identifier', 'submission')

    @classmethod
    def insert_quarterly_numbers(cls, submission_id=None):
        """Bulk insert tas quarterly finanical numbers."""
        field_list = [field.column for field in AppropriationAccountBalancesQuarterly._meta.get_fields()]

        # delete existing quarterly data
        if submission_id is None:
            AppropriationAccountBalancesQuarterly.objects.all().delete()
        else:
            AppropriationAccountBalancesQuarterly.objects.filter(submission_id=submission_id).delete()

        # retrieve RawQuerySet of quarterly breakouts
        qtr_records = AppropriationAccountBalances.get_quarterly_numbers(submission_id)
        qtr_list = []

        # for each record in the RawQuerySet, a corresponding AppropriationAccountBalancesQuarterly object and save it
        # for subsequent bulk insert
        # TODO: maybe we don't want this entire list in memory?
        for rec in qtr_records:
            # remove any fields in the qtr_record RawQuerySet object so that we can create its
            # AppropriationAccountBalancesQuarterly counterpart more easily
            rec_dict = rec.__dict__
            rec_dict = {key: rec_dict[key] for key in rec_dict if key in field_list}
            qtr_list.append(AppropriationAccountBalancesQuarterly(**rec_dict))

        AppropriationAccountBalancesQuarterly.objects.bulk_create(qtr_list)


class BudgetAuthority(models.Model):

    agency_identifier = models.TextField(db_index=True)  # aka CGAC
    fr_entity_code = models.TextField(null=True, db_index=True)  # aka FREC
    year = models.IntegerField(null=False)
    amount = models.BigIntegerField(null=True)

    class Meta:

        db_table = "budget_authority"
        unique_together = (("agency_identifier", "fr_entity_code", "year"),)
