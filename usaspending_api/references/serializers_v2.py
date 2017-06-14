from rest_framework import serializers
from usaspending_api.references.models import (
    Agency, Cfda, LegalEntity, Location, ObjectClass, OfficeAgency,
    RefProgramActivity, SubtierAgency, ToptierAgency, LegalEntityOfficers, Definition)
from usaspending_api.common.serializers import LimitableSerializer


class AgencyV2Serializer(serializers.Serializer):

    agency_name = serializers.CharField()
    active_fiscal_year = serializers.IntegerField(blank=True, null=True)
