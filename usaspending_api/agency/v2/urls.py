from django.urls import include, path, re_path
from usaspending_api.agency.v2.views.agency_overview import AgencyOverview
from usaspending_api.agency.v2.views.budget_function_count import BudgetFunctionCount
from usaspending_api.agency.v2.views.budget_function import BudgetFunctionList
from usaspending_api.agency.v2.views.budgetary_resources import BudgetaryResources
from usaspending_api.agency.v2.views.object_class_count import ObjectClassCount
from usaspending_api.agency.v2.views.federal_account_count import FederalAccountCount
from usaspending_api.agency.v2.views.federal_account_list import FederalAccountList
from usaspending_api.agency.v2.views.object_class_list import ObjectClassList
from usaspending_api.agency.v2.views.obligations_by_award_category import ObligationsByAwardCategory
from usaspending_api.agency.v2.views.program_activity_count import ProgramActivityCount
from usaspending_api.agency.v2.views.program_activity_list import ProgramActivityList
from usaspending_api.agency.v2.views.recipients import RecipientList
from usaspending_api.agency.v2.views.awards import Awards

urlpatterns = [
    re_path(
        "(?P<toptier_code>[0-9]{3,4})/",
        include(
            [
                path("", AgencyOverview.as_view()),
                path("budget_function/", BudgetFunctionList.as_view()),
                path("budget_function/count/", BudgetFunctionCount.as_view()),
                path("budgetary_resources/", BudgetaryResources.as_view()),
                path("federal_account/", FederalAccountList.as_view()),
                path("federal_account/count/", FederalAccountCount.as_view()),
                path("object_class/", ObjectClassList.as_view()),
                path("object_class/count/", ObjectClassCount.as_view()),
                path("obligations_by_award_category/", ObligationsByAwardCategory.as_view()),
                path("program_activity/", ProgramActivityList.as_view()),
                path("program_activity/count/", ProgramActivityCount.as_view()),
                path("recipients/", RecipientList.as_view()),
                path("awards/", Awards.as_view()),
            ]
        ),
    )
]
