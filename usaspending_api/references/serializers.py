from rest_framework import serializers
from usaspending_api.references.models import *
from usaspending_api.common.serializers import LimitableSerializer


class ToptierAgencySerializer(LimitableSerializer):

    class Meta:
        model = ToptierAgency
        fields = '__all__'


class SubtierAgencySerializer(LimitableSerializer):

    class Meta:
        model = SubtierAgency
        fields = '__all__'


class OfficeAgencySerializer(LimitableSerializer):

    class Meta:
        model = OfficeAgency
        fields = '__all__'


class AgencySerializer(LimitableSerializer):

    class Meta:
        model = Agency
        fields = '__all__'
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
        model = CFDAProgram
        fields = '__all__'


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


class RefProgramActivityBriefSerializer(serializers.ModelSerializer):
    prefetchable = False
    code = serializers.CharField(source='program_activity_code')
    title = serializers.CharField(source='program_activity_name')

    class Meta:

        model = RefProgramActivity
        fields = ('id', 'code', 'title')


class ObjectClassBriefSerializer(serializers.ModelSerializer):
    prefetchable = False
    major_object_class_code = serializers.CharField(source='major_object_class')

    class Meta:

        model = ObjectClass
        fields = ('major_object_class_code', 'major_object_class_name', )
