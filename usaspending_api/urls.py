"""usaspending_api URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/1.11/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  url(r'^$', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  url(r'^$', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.conf.urls import url, include
    2. Add a URL to urlpatterns:  url(r'^blog/', include('blog.urls'))
"""
from django.conf import settings
from django.conf.urls import url, include
from usaspending_api import views as views
from usaspending_api.common.views import MarkdownView
from django.conf.urls.static import static


urlpatterns = [
    url(r"^$", MarkdownView.as_view(markdown="landing_page.md")),
    url(r"^api/v1/accounts/", include("usaspending_api.accounts.urls")),
    url(r"^api/v1/awards/", include("usaspending_api.awards.v1.urls_awards")),
    url(r"^api/v1/federal_accounts/", include("usaspending_api.accounts.urls_federal_account")),
    url(r"^api/v1/references/", include("usaspending_api.references.v1.urls")),
    url(r"^api/v1/subawards/", include("usaspending_api.awards.v1.urls_subawards")),
    url(r"^api/v1/submissions/", include("usaspending_api.submissions.urls")),
    url(r"^api/v1/tas/", include("usaspending_api.accounts.urls_tas")),
    url(r"^api/v1/transactions/", include("usaspending_api.awards.v1.urls_transactions")),
    url(r"^api/v2/autocomplete/", include("usaspending_api.references.v2.urls_autocomplete")),
    url(r"^api/v2/awards/", include("usaspending_api.awards.v2.urls_awards")),
    url(r"^api/v2/awards/idvs/", include("usaspending_api.awards.v2.urls_idv_awards")),
    url(r"^api/v2/award_spending/", include("usaspending_api.awards.v2.urls_award_spending")),
    url(r"^api/v2/subawards/", include("usaspending_api.awards.v2.urls_subawards")),
    url(r"^api/v2/budget_authority/", include("usaspending_api.accounts.urls_budget_authority")),
    url(r"^api/v2/download/", include("usaspending_api.download.v2.urls")),
    url(r"^api/v2/budget_functions/", include("usaspending_api.accounts.v2.urls_budget_functions")),
    url(r"^api/v2/bulk_download/", include("usaspending_api.bulk_download.v2.urls")),
    url(r"^api/v2/federal_accounts/", include("usaspending_api.accounts.urls_federal_accounts_v2")),
    url(r"^api/v2/federal_obligations/", include("usaspending_api.accounts.urls_federal_obligations")),
    url(r"^api/v2/financial_balances/", include("usaspending_api.accounts.urls_financial_balances")),
    url(r"^api/v2/financial_spending/", include("usaspending_api.accounts.urls_financial_spending")),
    url(r"^api/v2/recipient/", include("usaspending_api.recipient.v2.urls")),
    url(r"^api/v2/references/", include("usaspending_api.references.v2.urls")),
    url(r"^api/v2/search/", include("usaspending_api.search.v2.urls_search")),
    url(r"^api/v2/spending/", include("usaspending_api.spending_explorer.v2.urls")),
    url(r"^api/v2/transactions/", include("usaspending_api.awards.v2.urls_transactions")),
    url(r"^api-auth/", include("rest_framework.urls", namespace="rest_framework")),
    url(r"^docs/", include("usaspending_api.api_docs.urls")),
    url(r"^status/", views.StatusView.as_view()),
] + static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)


if settings.DEBUG:
    import debug_toolbar

    urlpatterns += [url(r"^__debug__/", include(debug_toolbar.urls))]
