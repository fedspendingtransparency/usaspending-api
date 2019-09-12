from django.conf.urls import url

from usaspending_api.awards.v2.views.accounts import AwardAccountsViewSet
from usaspending_api.awards.v2.views.awards import AwardLastUpdatedViewSet, AwardRetrieveViewSet
from usaspending_api.awards.v2.views.funding_rollup import AwardFundingRollupViewSet
from usaspending_api.awards.v2.views.transaction_count import TransactionCountRetrieveViewSet
from usaspending_api.awards.v2.views.subaward_count import SubawardCountRetrieveViewSet
from usaspending_api.awards.v2.views.federal_accounts_count import FederalAccountCountRetrieveViewSet

urlpatterns = [
    url(r"^accounts/$", AwardAccountsViewSet.as_view()),
    url(r"^funding_rollup/$", AwardFundingRollupViewSet.as_view()),
    url(r"^last_updated", AwardLastUpdatedViewSet.as_view()),
    url(r"^(?P<requested_award>[A-Za-z0-9_. -]+)/$", AwardRetrieveViewSet.as_view()),
    url(r"^transaction_count/(?P<requested_award>[A-Za-z0-9_. -]+)/$", TransactionCountRetrieveViewSet.as_view()),
    url(r"^subaward_count/(?P<requested_award>[A-Za-z0-9_. -]+)/$", SubawardCountRetrieveViewSet.as_view()),
    url(
        r"^federal_account_count/(?P<requested_award>[A-Za-z0-9_. -]+)/$", FederalAccountCountRetrieveViewSet.as_view()
    ),
]
