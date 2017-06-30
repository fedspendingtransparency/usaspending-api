from rest_framework import serializers


class RecipientAutocompleteSerializer(serializers.Serializer):

    legal_entity_id = serializers.IntegerField()
    recipient_name = serializers.CharField()
    recipient_unique_id = serializers.CharField()
