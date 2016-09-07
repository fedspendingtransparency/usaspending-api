from rest_framework import serializers
from usaspending_api.submissions.models import SubmissionAttributes
from usaspending_api.submissions.models import SubmissionProcess


class SubmissionAttributesSerializer(serializers.ModelSerializer):

    class Meta:

        model = SubmissionAttributes
        fields = ('submission_id', 'cgac_code')

class SubmissionProcessSerializer(serializers.ModelSerializer):

    class Meta:

        model = SubmissionProcess
        fields = ('submission_process_id', 'status')
