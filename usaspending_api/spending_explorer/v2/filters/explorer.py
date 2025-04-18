from django.db.models import Exists, F, OuterRef, Sum, TextField, Value

from usaspending_api.common.calculations.file_b import FileBCalculations
from usaspending_api.references.models import Agency
from usaspending_api.submissions.models import SubmissionAttributes


class Explorer(object):

    file_b_calculations = FileBCalculations()

    def __init__(self, alt_set, queryset):
        # Moving agency mapping outside function to reduce response time
        agency_queryet = (
            Agency.objects.filter(toptier_flag=True)
            .values("id", "toptier_agency__toptier_code")
            .annotate(
                link=Exists(SubmissionAttributes.objects.filter(toptier_code=OuterRef("toptier_agency__toptier_code")))
            )
        )
        self.agency_ids = {
            agency["toptier_agency__toptier_code"]: {"id": agency["id"], "link": agency["link"]}
            for agency in agency_queryet
        }
        self.alt_set = alt_set
        self.queryset = queryset

    def budget_function(self):
        # Budget Function Queryset
        queryset = (
            self.queryset.annotate(
                id=F("treasury_account__budget_function_code"),
                type=Value("budget_function", output_field=TextField()),
                name=F("treasury_account__budget_function_title"),
                code=F("treasury_account__budget_function_code"),
            )
            .values("id", "type", "name", "code", "amount")
            .annotate(total=Sum(self.file_b_calculations.get_obligations()))
            .order_by("-total")
        )

        return queryset

    def budget_subfunction(self):
        # Budget Sub Function Queryset
        queryset = (
            self.queryset.annotate(
                id=F("treasury_account__budget_subfunction_code"),
                type=Value("budget_subfunction", output_field=TextField()),
                name=F("treasury_account__budget_subfunction_title"),
                code=F("treasury_account__budget_subfunction_code"),
            )
            .values("id", "type", "name", "code", "amount")
            .annotate(total=Sum(self.file_b_calculations.get_obligations()))
            .order_by("-total")
        )

        return queryset

    def federal_account(self):
        # Federal Account Queryset
        queryset = (
            self.queryset.annotate(
                id=F("treasury_account__federal_account"),
                account_number=F("treasury_account__federal_account__federal_account_code"),
                type=Value("federal_account", output_field=TextField()),
                name=F("treasury_account__federal_account__account_title"),
                code=F("treasury_account__federal_account__main_account_code"),
            )
            .values("id", "account_number", "type", "name", "code", "amount")
            .annotate(total=Sum(self.file_b_calculations.get_obligations()))
            .order_by("-total")
        )

        return queryset

    def program_activity(self):
        # Program Activity Queryset
        queryset = (
            self.queryset.annotate(
                id=F("program_activity"),
                type=Value("program_activity", output_field=TextField()),
                name=F("program_activity__program_activity_name"),
                code=F("program_activity__program_activity_code"),
            )
            .values("id", "type", "name", "code", "amount")
            .annotate(total=Sum(self.file_b_calculations.get_obligations()))
            .order_by("-total")
        )

        return queryset

    def object_class(self):
        # Object Classes Queryset
        queryset = (
            self.queryset.annotate(
                id=F("object_class__major_object_class"),
                type=Value("object_class", output_field=TextField()),
                name=F("object_class__major_object_class_name"),
                code=F("object_class__major_object_class"),
            )
            .values("id", "type", "name", "code", "amount")
            .annotate(total=Sum(self.file_b_calculations.get_obligations()))
            .order_by("-total")
        )

        return queryset

    def recipient(self):
        # Recipients Queryset
        alt_set = (
            self.alt_set.filter(transaction_obligated_amount__isnull=False)
            .annotate(
                id=F("award__recipient_hash"),
                type=Value("recipient", output_field=TextField()),
                name=F("award__recipient_name"),
                code=F("award__recipient_name"),
            )
            .values("id", "type", "name", "code", "amount")
            .annotate(total=Sum("transaction_obligated_amount"))
            .order_by("-total")
        )

        return alt_set

    def agency(self):
        # Funding Top Tier Agencies Querysets
        queryset = (
            self.queryset.filter(treasury_account__funding_toptier_agency__isnull=False)
            .annotate(
                type=Value("agency", output_field=TextField()),
                name=F("treasury_account__funding_toptier_agency__name"),
                code=F("treasury_account__funding_toptier_agency__toptier_code"),
            )
            .values("type", "name", "code")
            .annotate(amount=Sum(self.file_b_calculations.get_obligations()))
            .order_by("-amount")
        )

        for element in queryset:
            element["id"] = self.agency_ids[element["code"]]["id"]
            element["link"] = self.agency_ids[element["code"]]["link"]
        return queryset

    def award_category(self):
        # Award Category Queryset
        alt_set = (
            self.alt_set.annotate(
                id=F("award__award_id"),
                type=Value("award_category", output_field=TextField()),
                name=F("award__category"),
            )
            .values("id", "type", "piid", "fain", "uri", "name", "amount")
            .annotate(total=Sum("transaction_obligated_amount"))
            .order_by("-total")
        )

        return alt_set

    def award(self):
        # Awards Queryset
        alt_set = (
            self.alt_set.annotate(
                id=F("award__award_id"),
                generated_unique_award_id=F("award__generated_unique_award_id"),
                type=Value("award", output_field=TextField()),
            )
            .values("id", "generated_unique_award_id", "type", "piid", "fain", "uri", "amount")
            .annotate(total=Sum("transaction_obligated_amount"))
            .order_by("-total")
        )

        return alt_set
