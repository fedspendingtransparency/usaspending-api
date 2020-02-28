from django.conf.urls import url

from usaspending_api.references.v2.views import agency, toptier_agencies, data_dictionary, glossary
from usaspending_api.references.v2.views.filter_trees import naics, tas

urlpatterns = [
    url(r"^agency/(?P<pk>[0-9]+)/$", agency.AgencyViewSet.as_view()),
    url(r"^toptier_agencies/$", toptier_agencies.ToptierAgenciesViewSet.as_view()),
    url(r"^data_dictionary/$", data_dictionary.DataDictionaryViewSet.as_view()),
    url(r"^glossary/$", glossary.GlossaryViewSet.as_view()),
    url(r"^naics/$", naics.NAICSViewSet.as_view()),
    url(r"^naics/(?P<requested_naics>[0-9]+)/$", naics.NAICSViewSet.as_view()),
    url(r"^filter_tree/tas/$", tas.TASViewSet.as_view()),
    url(r"^filter_tree/tas/(?P<tier1>\w*)/$", tas.TASViewSet.as_view()),
    url(r"^filter_tree/tas/(?P<tier1>\w*)/(?P<tier2>(\w|-)*)/$", tas.TASViewSet.as_view()),
    url(r"^filter_tree/tas/(?P<tier1>\w*)/(?P<tier2>(\w|-)*)/(?P<tier3>.*)/$", tas.TASViewSet.as_view()),
]
