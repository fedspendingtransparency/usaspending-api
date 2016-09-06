from rest_framework import serializers
from usaspending_api.awards.models import Award

class AwardSerializer(serializers.ModelSerializer):


    class Meta:


        model = Award
        fields = ('award_id', 'type')
