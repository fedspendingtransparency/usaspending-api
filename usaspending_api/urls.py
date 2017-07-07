"""usaspending_api URL Configuration

The `urlpatterns` list routes URLs to views. For more information please see:
    https://docs.djangoproject.com/en/1.10/topics/http/urls/
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
from usaspending_api.common.csv_views import CsvDownloadView
from django.conf.urls.static import static


urlpatterns = [
    url(r'^$', MarkdownView.as_view(markdown='landing_page.md')),
    url(r'^api/v1/accounts/', include('usaspending_api.accounts.urls')),
    url(r'^api/v1/awards/', include('usaspending_api.awards.urls_awards')),
    url(r'^api/v1/download/(?P<path>.*)', CsvDownloadView.as_view()),
    url(r'^api/v1/federal_accounts/', include('usaspending_api.accounts.urls_federal_account')),
    url(r'^api/v1/references/', include('usaspending_api.references.urls')),
    url(r'^api/v1/subawards/', include('usaspending_api.awards.urls_subawards')),
    url(r'^api/v1/submissions/', include('usaspending_api.submissions.urls')),
    url(r'^api/v1/tas/', include('usaspending_api.accounts.urls_tas')),
    url(r'^api/v1/transactions/', include('usaspending_api.awards.urls_transactions')),
    url(r'^api/v2/autocomplete/', include('usaspending_api.references.urls_autocomplete')),
    url(r'^api/v2/award_spending/', include('usaspending_api.awards.urls_award_spending')),
    url(r'^api/v2/budget_authority/', include('usaspending_api.accounts.urls_budget_authority')),
    url(r'^api/v2/federal_obligations/', include('usaspending_api.accounts.urls_federal_obligations')),
    url(r'^api/v2/financial_balances/', include('usaspending_api.accounts.urls_financial_balances')),
    url(r'^api/v2/financial_spending/', include('usaspending_api.accounts.urls_financial_spending')),
    url(r'^api/v2/references/', include('usaspending_api.references.urls_v2')),
    url(r'^api-auth/', include('rest_framework.urls', namespace='rest_framework')),
    url(r'^docs/', include('usaspending_api.api_docs.urls')),
    url(r'^status/', views.StatusView.as_view()),
] + static(settings.STATIC_URL, document_root=settings.STATIC_ROOT)


if settings.DEBUG:
    import debug_toolbar
    urlpatterns += [
        url(r'^__debug__/', include(debug_toolbar.urls)),
    ]
