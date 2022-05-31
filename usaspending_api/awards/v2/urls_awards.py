from django.urls import re_path

from usaspending_api.awards.v2.views.accounts import AwardAccountsViewSet
from usaspending_api.awards.v2.views.funding import AwardFundingViewSet
from usaspending_api.awards.v2.views.awards import AwardLastUpdatedViewSet, AwardRetrieveViewSet
from usaspending_api.awards.v2.views.funding_rollup import AwardFundingRollupViewSet
from usaspending_api.awards.v2.views.count.transaction_count import TransactionCountRetrieveViewSet
from usaspending_api.awards.v2.views.count.subaward_count import SubawardCountRetrieveViewSet
from usaspending_api.awards.v2.views.count.federal_accounts_count import FederalAccountCountRetrieveViewSet

award_id_regex = "(?P<requested_award>(((CONT|ASST)_(AWD|IDV|NON|AGG)_.+)|([0-9]+)))"

urlpatterns = [
    re_path(r"^accounts/$", AwardAccountsViewSet.as_view()),
    re_path(r"^funding/$", AwardFundingViewSet.as_view()),
    re_path(r"^funding_rollup/$", AwardFundingRollupViewSet.as_view()),
    re_path(r"^last_updated", AwardLastUpdatedViewSet.as_view()),
    re_path(r"^count/transaction/{}/$".format(award_id_regex), TransactionCountRetrieveViewSet.as_view()),
    re_path(r"^count/subaward/{}/$".format(award_id_regex), SubawardCountRetrieveViewSet.as_view()),
    re_path(r"^count/federal_account/{}/$".format(award_id_regex), FederalAccountCountRetrieveViewSet.as_view()),
    re_path(r"^{}/$".format(award_id_regex), AwardRetrieveViewSet.as_view()),
]
