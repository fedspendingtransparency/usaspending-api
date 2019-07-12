from usaspending_api.awards.v2.lookups.lookups import all_award_types_mappings
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.download.v2.base_download_viewset import BaseDownloadViewSet
from usaspending_api.references.models import ToptierAgency


class YearLimitedDownloadViewSet(BaseDownloadViewSet):
    """
    This route sends a request to the backend to begin generating a zipfile of award data in CSV form for download.

    endpoint_doc: /download/custom_award_data_download.md
    """

    def post(self, request):
        request.data['constraint_type'] = 'year'

        # TODO: update front end to use the Common Filter Object and get rid of this function
        self.process_filters(request.data)

        return BaseDownloadViewSet.post(self, request, 'award')

    def process_filters(self, request_data):
        """Filter function to update Bulk Download parameters to shared parameters"""

        # Validate filter parameter
        filters = request_data.get('filters', None)
        if not filters:
            raise InvalidParameterException('Missing one or more required body parameters: filters')

        # Validate keyword search first, remove all other filters
        keyword_filter = filters.get('keyword', None) or filters.get('keywords', None)
        if keyword_filter and len(filters.keys()) == 1:
            request_data['filters'] = {'elasticsearch_keyword': keyword_filter}
            return

        # Validate other parameters previously required by the Bulk Download endpoint
        for required_param in ['award_types', 'agency', 'date_type', 'date_range']:
            if required_param not in filters:
                raise InvalidParameterException('Missing one or more required body parameters: {}'.
                                                format(required_param))

        # Replacing award_types with award_type_codes
        filters['award_type_codes'] = []
        try:
            for award_type_code in filters['award_types']:
                if award_type_code in all_award_types_mappings:
                    filters['award_type_codes'].extend(all_award_types_mappings[award_type_code])
                else:
                    raise InvalidParameterException('Invalid award_type: {}'.format(award_type_code))
            del filters['award_types']
        except TypeError:
            raise InvalidParameterException('award_types parameter not provided as a list')

        # Replacing date_range with time_period
        date_range_copied = filters['date_range'].copy()
        date_range_copied['date_type'] = filters['date_type']
        filters['time_period'] = [date_range_copied]
        del filters['date_range']
        del filters['date_type']

        # Replacing agency with agencies
        if filters['agency'] != 'all':
            toptier_name = (ToptierAgency.objects.filter(toptier_agency_id=filters['agency']).values('name'))
            if not toptier_name:
                raise InvalidParameterException('Toptier ID not found: {}'.format(filters['agency']))
            toptier_name = toptier_name[0]['name']
            if 'sub_agency' in filters:
                if filters['sub_agency']:
                    filters['agencies'] = [{'type': 'awarding', 'tier': 'subtier', 'name': filters['sub_agency'],
                                           'toptier_name': toptier_name}]
                del filters['sub_agency']
            else:
                filters['agencies'] = [{'type': 'awarding', 'tier': 'toptier', 'name': toptier_name}]
        del filters['agency']

        request_data['filters'] = filters
