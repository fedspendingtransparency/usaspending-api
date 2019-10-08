from django.apps import AppConfig
from django.utils.translation import ugettext_lazy as _


class AwardsConfig(AppConfig):
    name = "usaspending_api.agencies"
    verbose = _("Agencies")
