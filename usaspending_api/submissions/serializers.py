from rest_framework import serializers
from usaspending_api.submissions.models import SubmissionAttributes


class SubmissionAttributesSerializer(serializers.ModelSerializer):

    class Meta:

        model = SubmissionAttributes
        fields = '__all__'
