from rest_framework import serializers
from usaspending_api.awards.models import Award, TransactionFPDS
from usaspending_api.references.models import (Agency, LegalEntity, Location, LegalEntityOfficers,
                                               SubtierAgency, ToptierAgency, OfficeAgency)


class AwardTypeAwardSpendingSerializer(serializers.Serializer):
    award_category = serializers.CharField()
    obligated_amount = serializers.DecimalField(None, 2)


class RecipientSerializer(serializers.Serializer):
    recipient_id = serializers.IntegerField()
    recipient_name = serializers.CharField()


class RecipientAwardSpendingSerializer(serializers.Serializer):
    award_category = serializers.CharField()
    obligated_amount = serializers.DecimalField(None, 2)
    recipient = RecipientSerializer(source='*')


class LimitableSerializerV2(serializers.ModelSerializer):

    """Extends the model serializer to support field limiting."""
    def __init__(self, *args, **kwargs):

        # Grab any kwargs include and exclude fields, these are typically
        # passed in by a parent serializer to the child serializer
        include_fields = kwargs.pop('fields', [])

        # Initialize now that kwargs have been cleared
        super(LimitableSerializerV2, self).__init__(*args, **kwargs)

        if len(include_fields) == 0 and hasattr(self.Meta, "default_fields"):
            include_fields = self.Meta.default_fields

        # For the include list, we need to include the parent field of
        # any nested fields
        include_fields = include_fields + self.identify_missing_children(self.Meta.model, include_fields)

        # Create and initialize the child serializers
        if hasattr(self.Meta, "nested_serializers"):
            # Initialize the child serializers
            children = self.Meta.nested_serializers
            for field in children.keys():
                # Get child kwargs
                kwargs = children[field].get("kwargs", {})

                # Pop the default fields from the child serializer kwargs
                default_fields = kwargs.pop("default_fields", [])

                child_include_fields, matched_include = self.get_child_fields(field, include_fields)

                if len(child_include_fields) == 0:
                    child_include_fields = default_fields

                child_args = {
                    **kwargs,
                    "fields": child_include_fields,
                }
                self.fields[field] = children[field]["class"](**child_args)

        if len(include_fields) > 0:
            allowed = set(include_fields)
            existing = set(self.fields)
            for field_name in existing - allowed:
                # If we have a coded field, always include its description
                if field_name.split("_")[-1] == "description" and "_".join(field_name.split("_")[:-1]) in allowed:
                    continue
                self.fields.pop(field_name)

    # Returns a list of child names for fields in the field list
    # This is necessary so that if a user requests "recipient__recipient_name"
    # we also include "recipient" so that it is serialized
    def identify_missing_children(self, model, fields):
        children = []
        model_fields = [f.name for f in model._meta.get_fields()]
        for field in fields:
            split = field.split("__")
            if len(split) > 0 and split[0] in model_fields and split[0] not in fields:
                children.append(split[0])

        return children

    # Takes a child's name and a list of fields, and returns a set of fields
    # that belong to that child
    def get_child_fields(self, child, fields):
        # An included field is a child's field if the field begins with that
        # child's name and two underscores
        pattern = "{}__".format(child)

        matched = []
        child_fields = []
        for field in fields:
            if field[:len(pattern)] == pattern:
                child_fields.append(field[len(pattern):])
                matched.append(field)
        return child_fields, matched


class LegalEntityOfficersSerializerV2(LimitableSerializerV2):
    class Meta:
        model = LegalEntityOfficers
        fields = [
            "officer_1_name",
            "officer_1_amount",
            "officer_2_name",
            "officer_2_amount",
            "officer_3_name",
            "officer_3_amount",
            "officer_4_name",
            "officer_4_amount",
            "officer_5_name",
            "officer_5_amount",
        ]


class ToptierAgencySerializerV2(LimitableSerializerV2):

    class Meta:
        model = ToptierAgency
        fields = [
            "name",
            "abbreviation"
        ]


class SubtierAgencySerializerV2(LimitableSerializerV2):

    class Meta:
        model = SubtierAgency
        fields = [

            "name",
            "abbreviation"
        ]


class OfficeAgencySerializerV2(LimitableSerializerV2):

    class Meta:
        model = OfficeAgency
        fields = [
            "aac_code",
            "name"
        ]


class LocationSerializerV2(LimitableSerializerV2):

    class Meta:
        model = Location
        fields = [
            "address_line1",
            "address_line2",
            "address_line3",
            "foreign_province",
            "city_name",
            "county_name",
            "state_code",
            "zip5",
            "zip4",
            "foreign_postal_code",
            "country_name",
            "location_country_code",
            "congressional_code"
        ]


class AgencySerializerV2(LimitableSerializerV2):
    office_agency_name = serializers.SerializerMethodField('office_agency_name_func')

    def office_agency_name_func(self, agency):
        if agency.office_agency:
            return agency.office_agency.name
        else:
            return None

    class Meta:
        model = Agency
        fields = [
            "toptier_agency",
            "subtier_agency",
            "office_agency_name",
        ]
        nested_serializers = {
            "toptier_agency": {
                "class": ToptierAgencySerializerV2,
                "kwargs": {"read_only": True}
            },
            "subtier_agency": {
                "class": SubtierAgencySerializerV2,
                "kwargs": {"read_only": True}
            },
            "office_agency": {
                "class": OfficeAgencySerializerV2,
                "kwargs": {"read_only": True}
            },
        }


class LegalEntitySerializerV2(LimitableSerializerV2):

    class Meta:
        model = LegalEntity
        fields = [
            "recipient_name",
            "recipient_unique_id",
            "parent_recipient_unique_id",
            "business_categories",
            "location"
        ]
        nested_serializers = {
            "location": {
                "class": LocationSerializerV2,
                "kwargs": {"read_only": True}
            }
        }


class LegalEntityOfficerPassThroughSerializerV2(LimitableSerializerV2):

    class Meta:
        model = LegalEntity
        fields = [
            "officers"
        ]
        nested_serializers = {
            "officers": {
                "class": LegalEntityOfficersSerializerV2,
                "kwargs": {"read_only": True}
            }
        }


class TransactionFPDSSerializerV2(LimitableSerializerV2):

    class Meta:
        model = TransactionFPDS
        fields = [
            'idv_type_description',
            'type_of_idc_description',
            'referenced_idv_agency_iden',
            'multiple_or_single_aw_desc',
            'solicitation_identifier',
            'solicitation_procedures',
            'number_of_offers_received',
            'extent_competed',
            'other_than_full_and_o_desc',
            'type_set_aside_description',
            'commercial_item_acquisitio',
            'commercial_item_test_desc',
            'evaluated_preference_desc',
            'fed_biz_opps_description',
            'small_business_competitive',
            'fair_opportunity_limi_desc',
            'product_or_service_code',
            'naics',
            'dod_claimant_program_code',
            'program_system_or_equipmen',
            'information_technolog_desc',
            'sea_transportation_desc',
            'clinger_cohen_act_pla_desc',
            'construction_wage_rat_desc',
            'labor_standards_descrip',
            'materials_supplies_descrip',
            'cost_or_pricing_data_desc',
            'domestic_or_foreign_e_desc',
            'foreign_funding_desc',
            'interagency_contract_desc',
            'major_program',
            'price_evaluation_adjustmen',
            'program_acronym',
            'subcontracting_plan',
            'multi_year_contract_desc',
            'purchase_card_as_paym_desc',
            'consolidated_contract_desc',
            'type_of_contract_pric_desc',
        ]


class AwardContractSerializerV2(LimitableSerializerV2):
    executive_details = serializers.SerializerMethodField("executive_details_func")
    period_of_performance = serializers.SerializerMethodField("period_of_performance_func")
    latest_transaction_contract_data = serializers.SerializerMethodField('latest_transaction_func')

    def latest_transaction_func(seld, award):
        return TransactionFPDSSerializerV2(award.latest_transaction.contract_data).data

    def period_of_performance_func(self, award):
        return {
                "period_of_performance_start_date": award.period_of_performance_start_date,
                "period_of_performance_current_end_date": award.period_of_performance_current_end_date
                }

    def executive_details_func(self, award):
        entity = LegalEntityOfficerPassThroughSerializerV2(award.recipient).data
        response = []
        if "officers" in entity and entity["officers"]:
            for x in range(1, 6):
                response.append({"name": entity["officers"]["officer_" + str(x) + "_name"],
                                "amount": entity["officers"]["officer_" + str(x) + "_amount"]})
        return {"officers": response}

    class Meta:

        model = Award
        fields = [
            "id",
            "type",
            "category",
            "type_description",
            "piid",
            "parent_award_piid",
            "description",
            "awarding_agency",
            "funding_agency",
            "recipient",
            "total_obligation",
            "base_and_all_options_value",
            "period_of_performance",
            "place_of_performance",
            "latest_transaction_contract_data",
            "subaward_count",
            "total_subaward_amount",
            "executive_details"
         ]
        nested_serializers = {
            "recipient": {
                "class": LegalEntitySerializerV2,
                "kwargs": {"read_only": True}
            },
            "awarding_agency": {
                "class": AgencySerializerV2,
                "kwargs": {"read_only": True}
            },
            "funding_agency": {
                "class": AgencySerializerV2,
                "kwargs": {"read_only": True}
            },
            "place_of_performance": {
                "class": LocationSerializerV2,
                "kwargs": {"read_only": True}
            },
        }


class AwardMiscSerializerV2(LimitableSerializerV2):
    period_of_performance = serializers.SerializerMethodField("period_of_performance_func")
    executive_details = serializers.SerializerMethodField("executive_details_func")
    cfda_objectives = serializers.SerializerMethodField('cfda_objectives_func')
    cfda_title = serializers.SerializerMethodField('cfda_title_func')
    cfda_number = serializers.SerializerMethodField('cfda_number_func')

    def cfda_objectives_func(self, award):
        return award.latest_transaction.assistance_data.cfda_objectives

    def cfda_title_func(self, award):
        return award.latest_transaction.assistance_data.cfda_title

    def cfda_number_func(self, award):
        return award.latest_transaction.assistance_data.cfda_number

    def period_of_performance_func(self, award):
        return {
                "period_of_performance_start_date": award.period_of_performance_start_date,
                "period_of_performance_current_end_date": award.period_of_performance_current_end_date
                }

    def executive_details_func(self, award):
        entity = LegalEntityOfficerPassThroughSerializerV2(award.recipient).data
        response = []
        if "officers" in entity and entity["officers"]:
            for x in range(1, 6):
                response.append({"name": entity["officers"]["officer_" + str(x) + "_name"],
                                 "amount": entity["officers"]["officer_" + str(x) + "_amount"]})
        return {"officers": response}

    class Meta:

        model = Award
        fields = [
            "id",
            "type",
            "category",
            "type_description",
            "piid",
            "description",
            "cfda_objectives",
            "cfda_number",
            "cfda_title",
            "awarding_agency",
            "funding_agency",
            "recipient",
            "subaward_count",
            "total_subaward_amount",
            "period_of_performance",
            "place_of_performance",
            "executive_details",
         ]
        nested_serializers = {
            "recipient": {
                "class": LegalEntitySerializerV2,
                "kwargs": {"read_only": True}
            },
            "awarding_agency": {
                "class": AgencySerializerV2,
                "kwargs": {"read_only": True}
            },
            "funding_agency": {
                "class": AgencySerializerV2,
                "kwargs": {"read_only": True}
            },
            "place_of_performance": {
                "class": LocationSerializerV2,
                "kwargs": {"read_only": True}
            }
        }
