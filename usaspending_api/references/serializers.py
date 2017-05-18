from rest_framework import serializers
from usaspending_api.references.models import (
    Agency, Cfda, LegalEntity, Location, ObjectClass, OfficeAgency,
    RefProgramActivity, SubtierAgency, ToptierAgency, LegalEntityOfficers, Definition)
from usaspending_api.common.serializers import LimitableSerializer


class ToptierAgencySerializer(LimitableSerializer):

    class Meta:
        model = ToptierAgency
        fields = '__all__'
        default_fields = [
            "cgac_code",
            "fpds_code",
            "name",
            "abbreviation"
        ]


class SubtierAgencySerializer(LimitableSerializer):

    class Meta:
        model = SubtierAgency
        fields = '__all__'
        default_fields = [
            "subtier_code",
            "name",
            "abbreviation"
        ]


class OfficeAgencySerializer(LimitableSerializer):

    class Meta:
        model = OfficeAgency
        fields = '__all__'
        default_fields = [
            "aac_code",
            "name"
        ]


class AgencySerializer(LimitableSerializer):

    class Meta:
        model = Agency
        fields = '__all__'
        default_fields = [
            "id",
            "toptier_agency",
            "subtier_agency",
            "office_agency",
            "toptier_flag"
        ]
        nested_serializers = {
            "toptier_agency": {
                "class": ToptierAgencySerializer,
                "kwargs": {"read_only": True}
            },
            "subtier_agency": {
                "class": SubtierAgencySerializer,
                "kwargs": {"read_only": True}
            },
            "office_agency": {
                "class": OfficeAgencySerializer,
                "kwargs": {"read_only": True}
            },
        }


class CfdaSerializer(LimitableSerializer):

    class Meta:
        model = Cfda
        fields = '__all__'
        default_fields = [
            "id",
            "program_number",
            "program_title",
            "popular_name",
            "website_address",
            "objectives",
        ]


class LocationSerializer(LimitableSerializer):

    class Meta:

        model = Location
        fields = '__all__'
        default_fields = [
            "address_line1",
            "address_line2",
            "address_line3",
            "city_name",
            "state_name",
            "country_name",
            "state_code",
            "location_country_code",
            "zip5",
            "foreign_province",
            "foreign_city_name",
            "foreign_postal_code"
        ]


class LegalEntityOfficersSerializer(LimitableSerializer):

    class Meta:
        model = LegalEntityOfficers
        fields = '__all__'
        default_fields = [
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


class LegalEntitySerializer(LimitableSerializer):

    class Meta:
        model = LegalEntity
        fields = '__all__'
        default_fields = [
            "legal_entity_id",
            "parent_recipient_unique_id",
            "recipient_unique_id",
            "recipient_name",
            "business_types",
            "business_types_description",
            "business_categories",
            "location"
        ]
        nested_serializers = {
            "location": {
                "class": LocationSerializer,
                "kwargs": {"read_only": True}
            },
            "officers": {
                "class": LegalEntityOfficersSerializer,
                "kwargs": {"read_only": True}
            }
        }


class ProgramActivitySerializer(LimitableSerializer):

    class Meta:

        model = RefProgramActivity
        fields = ('id', 'program_activity_code', 'program_activity_name')


class ObjectClassSerializer(LimitableSerializer):

    class Meta:

        model = ObjectClass
        fields = (
            'id', 'major_object_class', 'major_object_class_name',
            'object_class', 'object_class_name')


class DefinitionSerializer(LimitableSerializer):

    class Meta:

        model = Definition
        fields = ['term', 'slug', 'data_act_term', 'plain', 'official', 'resources']
