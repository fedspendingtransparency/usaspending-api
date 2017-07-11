from rest_framework import serializers


class AwardTypeAwardSpendingSerializer(serializers.Serializer):

    award_type = serializers.CharField()
    toptier_agency = serializers.CharField()
    subtier_agency = serializers.CharField()
    obligated_amount = serializers.DecimalField(None, 2)


class RecipientSeriallizer(serializers.Serializer):
        recipient_id = serializers.IntegerField()
        recipient_name = serializers.CharField()


class RecipientAwardSpendingSerializer(serializers.Serializer):
        recipient = RecipientSeriallizer(source='*')
        toptier_agency = serializers.CharField()
        subtier_agency = serializers.CharField()
        obligated_amount = serializers.DecimalField(None, 2)
