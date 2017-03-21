from collections import defaultdict
from decimal import Decimal

from django.db import models

from usaspending_api.common.helpers import fy
from usaspending_api.submissions.models import SubmissionAttributes
from usaspending_api.common.models import DataSourceTrackedModel


# Table #3 - Treasury Appropriation Accounts.
class TreasuryAppropriationAccount(DataSourceTrackedModel):
    treasury_account_identifier = models.AutoField(primary_key=True)
    tas_rendering_label = models.CharField(max_length=50, blank=True, null=True)
    allocation_transfer_agency_id = models.CharField(max_length=3, blank=True, null=True)
    agency_id = models.CharField(max_length=3)
    beginning_period_of_availability = models.CharField(max_length=4, blank=True, null=True)
    ending_period_of_availability = models.CharField(max_length=4, blank=True, null=True)
    availability_type_code = models.CharField(max_length=1, blank=True, null=True)
    main_account_code = models.CharField(max_length=4)
    sub_account_code = models.CharField(max_length=3)
    account_title = models.CharField(max_length=300, blank=True, null=True)
    reporting_agency_id = models.CharField(max_length=3, blank=True, null=True)
    reporting_agency_name = models.CharField(max_length=100, blank=True, null=True)
    budget_bureau_code = models.CharField(max_length=6, blank=True, null=True)
    budget_bureau_name = models.CharField(max_length=100, blank=True, null=True)
    fr_entity_code = models.CharField(max_length=4, blank=True, null=True)
    fr_entity_description = models.CharField(max_length=100, blank=True, null=True)
    budget_function_code = models.CharField(max_length=3, blank=True, null=True)
    budget_function_title = models.CharField(max_length=100, blank=True, null=True)
    budget_subfunction_code = models.CharField(max_length=3, blank=True, null=True)
    budget_subfunction_title = models.CharField(max_length=100, blank=True, null=True)
    drv_appropriation_availability_period_start_date = models.DateField(blank=True, null=True)
    drv_appropriation_availability_period_end_date = models.DateField(blank=True, null=True)
    drv_appropriation_account_expired_status = models.CharField(max_length=10, blank=True, null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)

    @staticmethod
    def generate_tas_rendering_label(ATA, AID, TYPECODE, BPOA, EPOA, MAC, SUB):
        ATA = ATA.strip()
        AID = AID.strip()
        TYPECODE = TYPECODE.strip()
        BPOA = BPOA.strip()
        EPOA = EPOA.strip()
        MAC = MAC.strip()
        SUB = SUB.strip().lstrip("0")

        # print("ATA: " + ATA + "\nAID: " + AID + "\nTYPECODE: " + TYPECODE + "\nBPOA: " + BPOA + "\nEPOA: " + EPOA + "\nMAC: " + MAC + "\nSUB: " + SUB)

        # Attach hyphen to ATA if it exists
        if ATA:
            ATA = ATA + "-"

        POAPHRASE = BPOA
        # If we have BOTH BPOA and EPOA
        if BPOA and EPOA:
            # And they're equal
            if not BPOA == EPOA:
                POAPHRASE = BPOA + "/" + EPOA

        ACCTPHRASE = MAC
        if SUB:
            ACCTPHRASE = ACCTPHRASE + "." + SUB

        concatenated_tas = ATA + AID + TYPECODE + POAPHRASE + ACCTPHRASE
        return concatenated_tas

    @staticmethod
    def get_default_fields(path=None):
        if "awards" in path:
            return [
                "treasury_account_identifier",
                "tas_rendering_label",
                "account_title",
                "reporting_agency_id",
                "reporting_agency_name",
            ]

        return [
            "treasury_account_identifier",
            "tas_rendering_label",
            "allocation_transfer_agency_id",
            "agency_id",
            "beginning_period_of_availability",
            "ending_period_of_availability",
            "availability_type_code",
            "main_account_code",
            "sub_account_code",
            "account_title",
            "reporting_agency_id",
            "reporting_agency_name",
            "budget_bureau_code",
            "budget_bureau_name",
            "fr_entity_code",
            "fr_entity_description",
            "budget_function_code",
            "budget_function_title",
            "budget_subfunction_code",
            "budget_subfunction_title",
            "account_balances",
            "program_balances",
            "program_activities",
            "object_classes",
            "totals_program_activity",
            "totals_object_class",
            "totals",
        ]

    @property
    def program_activities(self):
        return [
            pb.program_activity_code
            for pb in self.program_balances.distinct('program_activity_code')
        ]

    @property
    def future_object_classes(self):
        results = []
        return results
        """TODO: Once FinancialAccountsByProgramActivityObjectClass.object_class
        has been fixed to point to ObjectClass instead of RefObjectClassCode,
        this will work with:
            return [pb.object_class for pb in self.program_balances.distinct('object_class')]
        """

    @property
    def object_classes(self):
        return [
            pb.object_class
            for pb in self.program_balances.distinct('object_class')
        ]

    @property
    def totals_object_class(self):
        results = []
        for object_class in self.object_classes:
            obligations = defaultdict(Decimal)
            outlays = defaultdict(Decimal)
            for pb in self.program_balances.filter(object_class=object_class):
                reporting_fiscal_year = fy(
                    pb.submission.reporting_period_start)
                obligations[
                    reporting_fiscal_year] += pb.obligations_incurred_by_program_object_class_cpe
                outlays[
                    reporting_fiscal_year] += pb.gross_outlay_amount_by_program_object_class_cpe
            result = {
                'major_object_class_code': None,
                'major_object_class_name':
                None,  # TODO: enable once ObjectClass populated
                'object_class': object_class.object_class,  # TODO: remove
                'outlays': obligations,
                'obligations': outlays,
            }
            results.append(result)
        return results

    @property
    def totals_program_activity(self):
        results = []
        for pa in self.program_activities:
            obligations = defaultdict(Decimal)
            outlays = defaultdict(Decimal)
            for pb in self.program_balances.filter(program_activity_code=pa):
                reporting_fiscal_year = fy(
                    pb.submission.reporting_period_start)
                # TODO: once it is present, use the reporting_fiscal_year directly
                obligations[
                    reporting_fiscal_year] += pb.obligations_incurred_by_program_object_class_cpe
                outlays[
                    reporting_fiscal_year] += pb.gross_outlay_amount_by_program_object_class_cpe
            result = {
                'id': pa.ref_program_activity_id,
                'program_activity_name': pa.program_activity_name,
                'program_activity_code': pa.program_activity_code,
                'obligations': obligations,
                'outlays': outlays,
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
            'outgoing': {
                'outlays': outlays,
                'obligations': obligations,
                'budget_authority': budget_authority,
            },
            'incoming': {}
        }
        return results

    class Meta:
        managed = True
        db_table = 'treasury_appropriation_account'

    def __str__(self):
        return "%s" % (self.tas_rendering_label)


# Table #4 - Appropriation Account Balances
class AppropriationAccountBalances(DataSourceTrackedModel):
    appropriation_account_balances_id = models.AutoField(primary_key=True)
    treasury_account_identifier = models.ForeignKey('TreasuryAppropriationAccount', models.CASCADE, db_column='treasury_account_identifier', related_name="account_balances")
    submission = models.ForeignKey(SubmissionAttributes, models.CASCADE)
    budget_authority_unobligated_balance_brought_forward_fyb = models.DecimalField(max_digits=21, decimal_places=2, blank=True, null=True)
    adjustments_to_unobligated_balance_brought_forward_cpe = models.DecimalField(max_digits=21, decimal_places=2)
    budget_authority_appropriated_amount_cpe = models.DecimalField(max_digits=21, decimal_places=2)
    borrowing_authority_amount_total_cpe = models.DecimalField(max_digits=21, decimal_places=2, blank=True, null=True)
    contract_authority_amount_total_cpe = models.DecimalField(max_digits=21, decimal_places=2, blank=True, null=True)
    spending_authority_from_offsetting_collections_amount_cpe = models.DecimalField(max_digits=21, decimal_places=2, blank=True, null=True)
    other_budgetary_resources_amount_cpe = models.DecimalField(max_digits=21, decimal_places=2, blank=True, null=True)
    budget_authority_available_amount_total_cpe = models.DecimalField(max_digits=21, decimal_places=2)
    gross_outlay_amount_by_tas_cpe = models.DecimalField(max_digits=21, decimal_places=2)
    deobligations_recoveries_refunds_by_tas_cpe = models.DecimalField(max_digits=21, decimal_places=2)
    unobligated_balance_cpe = models.DecimalField(max_digits=21, decimal_places=2)
    status_of_budgetary_resources_total_cpe = models.DecimalField(max_digits=21, decimal_places=2)
    obligations_incurred_total_by_tas_cpe = models.DecimalField(max_digits=21, decimal_places=2)
    drv_appropriation_availability_period_start_date = models.DateField(blank=True, null=True)
    drv_appropriation_availability_period_end_date = models.DateField(blank=True, null=True)
    drv_appropriation_account_expired_status = models.CharField(max_length=10, blank=True, null=True)
    tas_rendering_label = models.CharField(max_length=22, blank=True, null=True)
    drv_obligations_unpaid_amount = models.DecimalField(max_digits=21, decimal_places=2, blank=True, null=True)
    drv_other_obligated_amount = models.DecimalField(max_digits=21, decimal_places=2, blank=True, null=True)
    reporting_period_start = models.DateField(blank=True, null=True)
    reporting_period_end = models.DateField(blank=True, null=True)
    last_modified_date = models.DateField(blank=True, null=True)
    certified_date = models.DateField(blank=True, null=True)
    create_date = models.DateTimeField(auto_now_add=True, blank=True, null=True)
    update_date = models.DateTimeField(auto_now=True, null=True)

    class Meta:
        managed = True
        db_table = 'appropriation_account_balances'
