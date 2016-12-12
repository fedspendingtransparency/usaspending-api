from rest_framework import serializers
from usaspending_api.references.models import *
from usaspending_api.common.serializers import LimitableSerializer


class AgencySerializer(serializers.ModelSerializer):

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
