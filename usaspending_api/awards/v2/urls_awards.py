from django.conf.urls import url

from usaspending_api.awards.v2.views.accounts import AwardAccountsViewSet
from usaspending_api.awards.v2.views.funding import AwardFundingViewSet
from usaspending_api.awards.v2.views.awards import AwardLastUpdatedViewSet, AwardRetrieveViewSet
from usaspending_api.awards.v2.views.funding_rollup import AwardFundingRollupViewSet
from usaspending_api.awards.v2.views.count.transaction_count import TransactionCountRetrieveViewSet
from usaspending_api.awards.v2.views.count.subaward_count import SubawardCountRetrieveViewSet
from usaspending_api.awards.v2.views.count.federal_accounts_count import FederalAccountCountRetrieveViewSet

urlpatterns = [
    url(r"^accounts/$", AwardAccountsViewSet.as_view()),
    url(r"^funding/$", AwardFundingViewSet.as_view()),
    url(r"^funding_rollup/$", AwardFundingRollupViewSet.as_view()),
    url(r"^last_updated", AwardLastUpdatedViewSet.as_view()),
    url(r"^count/transaction/(?P<requested_award>[0-9]+)/$", TransactionCountRetrieveViewSet.as_view()),
    url(r"^count/transaction/(?P<requested_award>(CONT|ASST)_(AWD|IDV|NON|AGG)_.+)/$", TransactionCountRetrieveViewSet.as_view()),
    url("^count/subaward/(?P<requested_award>[0-9]+)/$", SubawardCountRetrieveViewSet.as_view()),
    url("^count/subaward/(?P<requested_award>(CONT|ASST)_(AWD|IDV|NON|AGG)_.+)/$", SubawardCountRetrieveViewSet.as_view()),
    url(
        "^count/federal_account/(?P<requested_award>[0-9]+)/$", FederalAccountCountRetrieveViewSet.as_view()
    ),

    url(
        "^count/federal_account/(?P<requested_award>(CONT|ASST)_(AWD|IDV|NON|AGG)_.+)/$", FederalAccountCountRetrieveViewSet.as_view()
    ),
    url(r"^(?P<requested_award>[0-9]+)/$", AwardRetrieveViewSet.as_view()),
    url("^(?P<requested_award>(CONT|ASST)_(AWD|IDV|NON|AGG)_.+)/$", AwardRetrieveViewSet.as_view()),
]
