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

class AgencySerializer(serializers.ModelSerializer):

    create_date = serializers.DateTimeField()
    update_date = serializers.DateTimeField()
    toptier_agency = ToptierAgencySerializer(read_only=True)
    subtier_agency = SubtierAgencySerializer(read_only=True)
    office_agency = OfficeAgencySerializer(read_only=True)

    class Meta:
        model = Agency
        fields = '__all__'


class LocationSerializer(LimitableSerializer):

    class Meta:

        model = Location
        fields = '__all__'


class LegalEntitySerializer(serializers.ModelSerializer):

    location = LocationSerializer(read_only=True)

    class Meta:
        model = LegalEntity
        fields = '__all__'
