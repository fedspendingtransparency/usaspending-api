from rest_framework import serializers
from usaspending_api.references.models import *
from usaspending_api.common.serializers import LimitableSerializer


class ToptierAgencySerializer(LimitableSerializer):

    class Meta:
        model = ToptierAgency
        fields = ('cgac_code', 'fpds_code', 'name')


class SubtierAgencySerializer(LimitableSerializer):

    class Meta:
        model = SubtierAgency
        fields = ('subtier_code', 'name')


class OfficeAgencySerializer(LimitableSerializer):

    class Meta:
        model = OfficeAgency
        fields = ('aac_code', 'name')


class AgencySerializer(LimitableSerializer):

    toptier_agency = ToptierAgencySerializer(read_only=True)
    subtier_agency = SubtierAgencySerializer(read_only=True)
    office_agency = OfficeAgencySerializer(read_only=True)

    class Meta:
        model = Agency
        fields = ('toptier_agency', 'subtier_agency', 'office_agency')


class LocationSerializer(LimitableSerializer):

    class Meta:

        model = Location
        fields = '__all__'


class LegalEntitySerializer(LimitableSerializer):

    class Meta:
        model = LegalEntity
        fields = '__all__'
        nested_serializers = {
            "location": {
                "class": LocationSerializer,
                "kwargs": {"read_only": True}
            },
        }
