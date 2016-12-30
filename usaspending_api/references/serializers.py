from rest_framework import serializers
from usaspending_api.references.models import *
from usaspending_api.common.serializers import LimitableSerializer


## AJ Replaced with LimitableSerializer
##class AgencySerializer(serializers.ModelSerializer):
class AgencySerializer(LimitableSerializer):

    class Meta:
        model = Agency
        fields = '__all__'


class LocationSerializer(LimitableSerializer):

    class Meta:
        model = Location
        fields = '__all__'


## AJ Replaced with LimitableSerializer
##class LegalEntitySerializer(serializers.ModelSerializer):
class LegalEntitySerializer(LimitableSerializer):

    location = LocationSerializer(read_only=True)

    class Meta:
        model = LegalEntity
        fields = '__all__'
