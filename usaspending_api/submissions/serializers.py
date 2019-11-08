from usaspending_api.common.serializers import LimitableSerializer
from usaspending_api.submissions.models import SubmissionAttributes


class SubmissionAttributesSerializer(LimitableSerializer):
    class Meta:

        model = SubmissionAttributes
        fields = "__all__"
        default_fields = ["submission_id", "toptier_code", "reporting_fiscal_year", "reporting_fiscal_quarter"]
