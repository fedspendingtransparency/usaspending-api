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


urlpatterns = [
    url(r'^status/', views.StatusView.as_view()),
    url(r'^api/v1/awards/', include('usaspending_api.awards.urls')),
    url(r'^api/v1/submissions/', include('usaspending_api.submissions.urls')),
    url(r'^api/v1/accounts/', include('usaspending_api.accounts.urls')),
    url(r'^api/v1/financial_activities/', include('usaspending_api.financial_activities.urls')),
    url(r'^api-auth/', include('rest_framework.urls', namespace='rest_framework')),
]

if settings.DEBUG:
    import debug_toolbar
    urlpatterns += [
        url(r'^__debug__/', include(debug_toolbar.urls)),
    ]
