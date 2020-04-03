from rest_framework import serializers


class RecipientSerializer(serializers.Serializer):
    recipient_name = serializers.CharField()


class RecipientAwardSpendingSerializer(serializers.Serializer):
    award_category = serializers.CharField()
    obligated_amount = serializers.DecimalField(None, 2)
    recipient = RecipientSerializer(source="*")
