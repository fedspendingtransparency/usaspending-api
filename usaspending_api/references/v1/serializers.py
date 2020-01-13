from rest_framework import serializers
from usaspending_api.references.models import (
    Agency,
    Cfda,
    Location,
    ObjectClass,
    RefProgramActivity,
    SubtierAgency,
    ToptierAgency,
)
from usaspending_api.common.serializers import LimitableSerializer


class ToptierAgencySerializer(LimitableSerializer):
    class Meta:
        model = ToptierAgency
        fields = "__all__"
        default_fields = ["toptier_code", "name", "abbreviation"]


class SubtierAgencySerializer(LimitableSerializer):
    class Meta:
        model = SubtierAgency
        fields = "__all__"
        default_fields = ["subtier_code", "name", "abbreviation"]


class AgencySerializer(LimitableSerializer):
    class Meta:
        model = Agency
        fields = "__all__"
        default_fields = ["id", "toptier_agency", "subtier_agency", "toptier_flag"]
        nested_serializers = {
            "toptier_agency": {"class": ToptierAgencySerializer, "kwargs": {"read_only": True}},
            "subtier_agency": {"class": SubtierAgencySerializer, "kwargs": {"read_only": True}},
        }


class CfdaSerializer(LimitableSerializer):
    class Meta:
        model = Cfda
        fields = "__all__"
        default_fields = ["id", "program_number", "program_title", "popular_name", "website_address", "objectives"]


class LocationSerializer(LimitableSerializer):
    class Meta:
        model = Location
        fields = "__all__"
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
            "foreign_postal_code",
        ]


class ProgramActivitySerializer(LimitableSerializer):
    class Meta:

        model = RefProgramActivity
        fields = ("id", "program_activity_code", "program_activity_name")


class ObjectClassSerializer(LimitableSerializer):
    class Meta:

        model = ObjectClass
        fields = ("id", "major_object_class", "major_object_class_name", "object_class", "object_class_name")


class FilterSerializer(serializers.Serializer):

    hash = serializers.CharField()


class HashSerializer(serializers.Serializer):

    json_str = serializers.CharField()
