from usaspending_api.accounts.serializers import TasSerializer
from usaspending_api.awards.models import FinancialAccountsByAwards
from usaspending_api.awards.models import TransactionFPDS, TransactionFABS
from usaspending_api.common.serializers import LimitableSerializer
from usaspending_api.references.v1.serializers import CfdaSerializer
from usaspending_api.references.v1.serializers import ProgramActivitySerializer, ObjectClassSerializer
from usaspending_api.submissions.serializers import SubmissionAttributesSerializer


class FinancialAccountsByAwardsSerializer(LimitableSerializer):
    class Meta:
        model = FinancialAccountsByAwards
        fields = "__all__"
        default_fields = [
            "financial_accounts_by_awards_id",
            "submission",
            "award",
            "treasury_account",
            "transaction_obligated_amount",
            "object_class",
            "program_activity",
            "piid",
            "fain",
            "uri",
            "gross_outlay_amount_by_award_cpe",
            "gross_outlay_amount_by_award_fyb",
            "certified_date",
            "last_modified_date",
        ]
        nested_serializers = {
            "treasury_account": {
                "class": TasSerializer,
                "kwargs": {
                    "read_only": True,
                    "default_fields": [
                        "treasury_account_identifier",
                        "tas_rendering_label",
                        "account_title",
                        "reporting_agency_id",
                        "reporting_agency_name",
                        "federal_account",
                        "funding_toptier_agency",
                        "awarding_toptier_agency",
                    ],
                },
            },
            "program_activity": {"class": ProgramActivitySerializer, "kwargs": {"read_only": True}},
            "object_class": {"class": ObjectClassSerializer, "kwargs": {"read_only": True}},
            "submission": {"class": SubmissionAttributesSerializer, "kwargs": {"read_only": True}},
        }


class TransactionFABSSerializer(LimitableSerializer):

    prefetchable = False

    class Meta:
        model = TransactionFABS

        # Unforuntately, we can't use fields = '__all__', because we need to add
        # a property to that
        fields = [
            # fields in database table
            "action_date",
            "action_type",
            "action_type_description",
            "afa_generated_unique",
            "assistance_type",
            "assistance_type_desc",
            "award_description",
            "award_modification_amendme",
            "awardee_or_recipient_legal",
            "awardee_or_recipient_uniqu",
            "ultimate_parent_legal_enti",
            "ultimate_parent_unique_ide",
            "awarding_agency_code",
            "awarding_agency_name",
            "awarding_office_code",
            "awarding_office_name",
            "awarding_sub_tier_agency_c",
            "awarding_sub_tier_agency_n",
            "business_funds_indicator",
            "business_funds_ind_desc",
            "business_types",
            "business_types_desc",
            "cfda_number",
            "cfda_objectives",
            "cfda_title",
            "correction_delete_indicatr",
            "correction_delete_ind_desc",
            "created_at",
            "face_value_loan_guarantee",
            "fain",
            "federal_action_obligation",
            "fiscal_year_and_quarter_co",
            "funding_agency_code",
            "funding_agency_name",
            "funding_office_code",
            "funding_office_name",
            "funding_sub_tier_agency_co",
            "funding_sub_tier_agency_na",
            "is_active",
            "is_historical",
            "legal_entity_address_line1",
            "legal_entity_address_line2",
            "legal_entity_address_line3",
            "legal_entity_city_code",
            "legal_entity_city_name",
            "legal_entity_congressional",
            "legal_entity_country_code",
            "legal_entity_country_name",
            "legal_entity_county_code",
            "legal_entity_county_name",
            "legal_entity_foreign_city",
            "legal_entity_foreign_posta",
            "legal_entity_foreign_provi",
            "legal_entity_state_code",
            "legal_entity_state_name",
            "legal_entity_zip5",
            "legal_entity_zip_last4",
            "modified_at",
            "non_federal_funding_amount",
            "original_loan_subsidy_cost",
            "period_of_performance_curr",
            "period_of_performance_star",
            "place_of_perform_country_c",
            "place_of_perform_country_n",
            "place_of_perform_county_co",
            "place_of_perform_county_na",
            "place_of_perform_state_nam",
            "place_of_performance_city",
            "place_of_performance_code",
            "place_of_performance_congr",
            "place_of_performance_forei",
            "place_of_performance_zip4a",
            "published_fabs_id",
            "record_type",
            "record_type_description",
            "refresh_from_db",
            "sai_number",
            "total_funding_amount",
            "transaction",
            "transaction_id",
            "updated_at",
            "uri",
            "officer_1_amount",
            "officer_1_name",
            "officer_2_amount",
            "officer_2_name",
            "officer_3_amount",
            "officer_3_name",
            "officer_4_amount",
            "officer_4_name",
            "officer_5_amount",
            "officer_5_name",
        ] + [
            # property fields manually added
            "cfda_objectives"
        ]
        # fields = '__all__'
        default_fields = [
            "fain",
            "uri",
            "cfda",
            "cfda_number",
            "cfda_title",
            "cfda_objectives",
            "face_value_loan_guarantee",
            "original_loan_subsidy_cost",
            "type",
        ]
        nested_serializers = {"cfda": {"class": CfdaSerializer, "kwargs": {"read_only": True}}}


class TransactionFPDSSerializer(LimitableSerializer):

    prefetchable = False

    class Meta:
        model = TransactionFPDS
        fields = "__all__"
        default_fields = [
            "piid",
            "parent_award_piid",
            "type",
            "type_description",
            "cost_or_pricing_data",
            "type_of_contract_pricing",
            "type_of_contract_pricing_description",
            "naics",
            "naics_description",
            "product_or_service_code",
        ]
