from django.db import models
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
            "account_balances"
        ]

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
