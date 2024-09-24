"""usaspending_api URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/2.2/topics/http/urls/
Examples:
Function views
    1. Add an import:  from my_app import views
    2. Add a URL to urlpatterns:  re_path(r'^$', views.home, name='home')
Class-based views
    1. Add an import:  from other_app.views import Home
    2. Add a URL to urlpatterns:  re_path(r'^$', Home.as_view(), name='home')
Including another URLconf
    1. Import the include() function: from django.urls import include, re_path
    2. Add a URL to urlpatterns:  re_path(r'^blog/', include('blog.urls'))
"""

from django.conf.urls.static import static
from django.urls import include, re_path
from usaspending_api import settings
from usaspending_api import views as views
from usaspending_api.common.views import MarkdownView


urlpatterns = [
    re_path(r"^$", MarkdownView.as_view(markdown="landing_page.md")),
    re_path(r"^api-auth/", include("rest_framework.urls", namespace="rest_framework")),
    re_path(r"^api/v1/accounts/", include("usaspending_api.accounts.urls")),
    re_path(r"^api/v1/awards/", include("usaspending_api.awards.v1.urls_awards")),
    re_path(r"^api/v1/federal_accounts/", include("usaspending_api.accounts.urls_federal_account")),
    re_path(r"^api/v1/references/", include("usaspending_api.references.v1.urls")),
    re_path(r"^api/v1/subawards/", include("usaspending_api.awards.v1.urls_subawards")),
    re_path(r"^api/v1/submissions/", include("usaspending_api.submissions.urls")),
    re_path(r"^api/v1/tas/", include("usaspending_api.accounts.urls_tas")),
    re_path(r"^api/v1/transactions/", include("usaspending_api.awards.v1.urls_transactions")),
    re_path(r"^api/v2/agency/", include("usaspending_api.agency.v2.urls")),
    re_path(r"^api/v2/autocomplete/", include("usaspending_api.references.v2.urls_autocomplete")),
    re_path(r"^api/v2/award_spending/", include("usaspending_api.awards.v2.urls_award_spending")),
    re_path(r"^api/v2/awards/", include("usaspending_api.awards.v2.urls_awards")),
    re_path(r"^api/v2/budget_authority/", include("usaspending_api.accounts.urls_budget_authority")),
    re_path(r"^api/v2/budget_functions/", include("usaspending_api.accounts.v2.urls_budget_functions")),
    re_path(r"^api/v2/bulk_download/", include("usaspending_api.bulk_download.v2.urls")),
    re_path(r"^api/v2/disaster/", include("usaspending_api.disaster.v2.urls")),
    re_path(r"^api/v2/download/", include("usaspending_api.download.v2.urls")),
    re_path(r"^api/v2/federal_accounts/", include("usaspending_api.accounts.urls_federal_accounts_v2")),
    re_path(r"^api/v2/federal_obligations/", include("usaspending_api.accounts.urls_federal_obligations")),
    re_path(r"^api/v2/financial_balances/", include("usaspending_api.accounts.urls_financial_balances")),
    re_path(r"^api/v2/financial_spending/", include("usaspending_api.accounts.urls_financial_spending")),
    re_path(r"^api/v2/idvs/", include("usaspending_api.idvs.v2.urls_idvs")),
    re_path(r"^api/v2/recipient/", include("usaspending_api.recipient.v2.urls")),
    re_path(r"^api/v2/references/", include("usaspending_api.references.v2.urls")),
    re_path(r"^api/v2/reporting/", include("usaspending_api.reporting.v2.urls")),
    re_path(r"^api/v2/search/", include("usaspending_api.search.v2.urls")),
    re_path(r"^api/v2/spending/", include("usaspending_api.spending_explorer.v2.urls")),
    re_path(r"^api/v2/subawards/", include("usaspending_api.awards.v2.urls_subawards")),
    re_path(r"^api/v2/transactions/", include("usaspending_api.awards.v2.urls_transactions")),
    re_path(r"^docs/", include("usaspending_api.api_docs.urls")),
    re_path(r"^status/", views.StatusView.as_view()),
] + static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)


if settings.DEBUG:
    import debug_toolbar

    urlpatterns += [re_path(r"^__debug__/", include(debug_toolbar.urls))]
