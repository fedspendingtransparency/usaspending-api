from rest_framework import serializers


class AwardTypeAwardSpendingSerializer(serializers.Serializer):
    award_type = serializers.CharField()
    obligated_amount = serializers.DecimalField(None, 2)


class RecipientAwardSpendingSerializer(serializers.Serializer):
    award__category = serializers.CharField()
    recipient_id = serializers.IntegerField()
    recipient_name = serializers.CharField()
    obligated_amount = serializers.DecimalField(None, 2)
