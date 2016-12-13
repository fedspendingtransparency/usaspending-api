from rest_framework import serializers
from usaspending_api.references.models import *
from usaspending_api.common.serializers import LimitableSerializer


class AgencySerializer(LimitableSerializer):

    class Meta:
        model = Agency
        fields = '__all__'


class LocationSerializer(LimitableSerializer):

    class Meta:

        model = Location
        fields = '__all__'


class LegalEntitySerializer(LimitableSerializer):

    location = serializers.SerializerMethodField()

    def get_location(self, obj):
        return self.get_subserializer_data(LocationSerializer, obj.location, 'location', read_only=True)

    class Meta:
        model = LegalEntity
        fields = '__all__'
