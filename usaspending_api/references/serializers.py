from rest_framework import serializers
from usaspending_api.references.models import (
    Agency, CFDAProgram, LegalEntity, Location, ObjectClass, OfficeAgency,
    RefProgramActivity, SubtierAgency, ToptierAgency, LegalEntityOfficers, Definition, DefinitionResource)
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


class LegalEntityOfficersSerializer(LimitableSerializer):

    class Meta:
        model = LegalEntityOfficers
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
        
        
class DefinitionResourceSerializer(serializers.Serializer):
    
    class Meta:
        model = DefinitionResource
        
        
class DefinitionSerializer(LimitableSerializer):

    resources = DefinitionResourceSerializer(many=True, read_only=True)
    
    class Meta:

        model = Definition
        fields = ['term', 'data_act_term', 'plain', 'official', 'resources']
