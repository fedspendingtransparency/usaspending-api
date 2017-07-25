from rest_framework import serializers


class AwardTypeAwardSpendingSerializer(serializers.Serializer):
    award_type = serializers.CharField()
    obligated_amount = serializers.DecimalField(None, 2)


class RecipientSerializer(serializers.Serializer):
    recipient_id = serializers.IntegerField()
    recipient_name = serializers.CharField()
    obligated_amount = serializers.DecimalField(None, 2)


class RecipientAwardSpendingSerializer(serializers.Serializer):
    award_category = serializers.CharField()
    recipient = RecipientSerializer(source='*')
