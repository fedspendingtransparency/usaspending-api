from usaspending_api.common.serializers import LimitableSerializer
from usaspending_api.submissions.models import SubmissionAttributes
from usaspending_api.submissions.models import SubmissionProcess


class SubmissionAttributesSerializer(LimitableSerializer):

    class Meta:

        model = SubmissionAttributes
        fields = '__all__'


class SubmissionProcessSerializer(LimitableSerializer):

    class Meta:

        model = SubmissionProcess
        fields = '__all__'
