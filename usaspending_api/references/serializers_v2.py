from rest_framework import serializers


class AgencyV2Serializer(serializers.Serializer):

    agency_name = serializers.CharField()
    active_fiscal_year = serializers.IntegerField(blank=True, null=True)
