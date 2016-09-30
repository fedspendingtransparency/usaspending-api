from rest_framework import serializers
from usaspending_api.references.models import *


class AgencySerializer(serializers.ModelSerializer):

    class Meta:
        model = Agency
        fields = '__all__'


class LocationSerializer(serializers.ModelSerializer):

    class Meta:

        model = Location
        fields = '__all__'


class LegalEntitySerializer(serializers.ModelSerializer):

    location = LocationSerializer(read_only=True)

    class Meta:
        model = LegalEntity
        fields = '__all__'
