from collections import defaultdict
from decimal import Decimal
from django.db import models
from django_cte import CTEManager

from usaspending_api.common.helpers.date_helper import fy
from usaspending_api.common.models import DataSourceTrackedModel
from usaspending_api.common.exceptions import UnprocessableEntityException


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
    agency_id = models.TextField()
    funding_toptier_agency = models.ForeignKey(
        "references.ToptierAgency",
        models.DO_NOTHING,
        null=True,
        help_text=(
            "The toptier agency under which this treasury account should appear in lists and dropdowns.  Not "
            "as simple as just mapping the AID to an agency, although AID does factor into the decision.  It was "
            "recently recommended we rename this to parent toptier agency, however that is a much more involved "
            "change so we're keeping current naming for now."
        ),
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
    internal_start_date = models.DateField(blank=True, null=True)
    internal_end_date = models.DateField(blank=True, null=True)

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

    @staticmethod
    def tas_rendering_label_to_component_dictionary(tas_rendering_label) -> dict:
        try:
            components = tas_rendering_label.split("-")
            if len(components) < 4 or len(components) > 5:
                raise Exception  # don't have to be specific here since this is being swallowed and replaced
            retval = {}
            # we go in reverse, since the first component is the only optional one
            retval["sub"] = components[-1]
            retval["main"] = components[-2]

            # the third component from the back can either be two years, or one character
            if len(components[-3]) > 1:
                dates = components[-3].split("/")
                retval["bpoa"] = dates[0]
                retval["epoa"] = dates[1]
            else:
                retval["a"] = components[-3]

            retval["aid"] = components[-4]

            # ata may or may not be present
            if len(components) > 4:
                retval["ata"] = components[-5]

            return retval
        except Exception:
            raise UnprocessableEntityException(
                f"Cannot parse provided TAS: {tas_rendering_label}. Valid examples: 000-2010/2010-0400-000, 009-X-1701-000, 019-011-X-1071-000"
            )

    @property
    def program_activities(self):
        return [pb.program_activity for pb in self.program_balances.distinct("program_activity")]

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

    objects = CTEManager()

    class Meta:
        managed = True
        db_table = "treasury_appropriation_account"

    def __str__(self):
        return self.tas_rendering_label
