import os
import pandas as pd

from django.conf import settings
from django.db.models import F
from rest_framework.response import Response

from usaspending_api.accounts.models import FederalAccount
from usaspending_api.references.models import Agency
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.common.views import APIDocumentationView
from usaspending_api.download.lookups import CFO_CGACS
from usaspending_api.references.models import ToptierAgency


class DownloadListAgenciesViewSet(APIDocumentationView):
    """
    This route lists all the agencies and the subagencies or federal accounts associated under specific agencies.

    endpoint_doc: /download/list_agencies.md
    """
    # Get list of agencies without duplicates
    modified_agencies_list = os.path.join(settings.BASE_DIR, 'usaspending_api', 'data',
                                          'user_selectable_agency_list.csv')
    sub_agencies_map = {}

    def pull_modified_agencies_cgacs_subtiers(self):
        # Get a dict of used subtiers and their associated CGAC code pulled from
        # modified_agencies_list
        with open(self.modified_agencies_list, encoding='Latin-1') as modified_agencies_list_csv:
            mod_gencies_list_df = pd.read_csv(modified_agencies_list_csv, dtype=str)
        mod_gencies_list_df = mod_gencies_list_df[['CGAC AGENCY CODE', 'SUBTIER CODE', 'FREC', 'IS_FREC']]
        mod_gencies_list_df['CGAC AGENCY CODE'] = mod_gencies_list_df['CGAC AGENCY CODE'].apply(lambda x: x.zfill(3))
        mod_gencies_list_df['FREC'] = mod_gencies_list_df['FREC'].apply(lambda x: x.zfill(4))
        for _, row in mod_gencies_list_df.iterrows():
            # cgac_code in the database can be either agency cgac or frec code (if a frec agency)
            self.sub_agencies_map[row['SUBTIER CODE']] = row['FREC'] \
                if row['IS_FREC'].upper() == 'TRUE' else row['CGAC AGENCY CODE']

    def post(self, request):
        """Return list of agencies if no POST data is provided.
        Otherwise, returns sub_agencies/federal_accounts associated with the agency provided"""
        response_data = {'agencies': [],
                         'sub_agencies': [],
                         'federal_accounts': []}
        if not self.sub_agencies_map:
            # populate the sub_agencies dictionary
            self.pull_modified_agencies_cgacs_subtiers()
        used_cgacs = set(self.sub_agencies_map.values())

        agency_id = None
        post_data = request.data
        if post_data:
            if 'agency' not in post_data:
                raise InvalidParameterException('Missing one or more required body parameters: agency')
            agency_id = post_data['agency']

        # Get all the top tier agencies
        toptier_agencies = list(ToptierAgency.objects.filter(cgac_code__in=used_cgacs)
                                .values('name', 'toptier_agency_id', 'cgac_code'))

        if not agency_id:
            # Return all the agencies if no agency id provided
            cfo_agencies = sorted(list(filter(lambda agency: agency['cgac_code'] in CFO_CGACS, toptier_agencies)),
                                  key=lambda agency: CFO_CGACS.index(agency['cgac_code']))
            other_agencies = sorted([agency for agency in toptier_agencies if agency not in cfo_agencies],
                                    key=lambda agency: agency['name'])
            response_data['agencies'] = {'cfo_agencies': cfo_agencies,
                                         'other_agencies': other_agencies}
        else:
            # Get the top tier agency object based on the agency id provided
            top_tier_agency = list(filter(lambda toptier: toptier['toptier_agency_id'] == agency_id, toptier_agencies))
            if not top_tier_agency:
                raise InvalidParameterException('Agency ID not found')
            top_tier_agency = top_tier_agency[0]
            # Get the sub agencies and federal accounts associated with that top tier agency
            # Removed distinct subtier_agency_name since removing subtiers with multiple codes that aren't in the
            # modified list
            response_data['sub_agencies'] = Agency.objects.filter(toptier_agency_id=agency_id)\
                .values(subtier_agency_name=F('subtier_agency__name'),
                        subtier_agency_code=F('subtier_agency__subtier_code'))\
                .order_by('subtier_agency_name')
            # Tried converting this to queryset filtering but ran into issues trying to
            # double check the right used subtier_agency by cross checking the cgac_code
            # see the last 2 lines of the list comprehension below
            response_data['sub_agencies'] = [subagency for subagency in response_data['sub_agencies']
                                             if subagency['subtier_agency_code'] in self.sub_agencies_map and
                                             self.sub_agencies_map[subagency['subtier_agency_code']] ==
                                             top_tier_agency['cgac_code']]
            for subagency in response_data['sub_agencies']:
                del subagency['subtier_agency_code']

            response_data['federal_accounts'] = FederalAccount.objects\
                .filter(agency_identifier=top_tier_agency['cgac_code'])\
                .values(federal_account_name=F('account_title'), federal_account_id=F('id'))\
                .order_by('federal_account_name')
        return Response(response_data)
