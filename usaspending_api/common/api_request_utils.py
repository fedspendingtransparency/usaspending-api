from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
from django.db.models import Q
from datetime import date


# This class represents a fiscal year
# Currently it functions only on the basis of federal fiscal years, more rules
# may be added for other jurisdictions
class FiscalYear():
    def __init__(self, fy):
        self.fy = fy
        # FY start previous year on Oct 1st. i.e. FY 2017 starts 10-1-2016
        self.fy_start_date = date(int(fy)-1, 10, 1)
        # FY ends current FY year on Sept 30th i.e. FY 2017 ends 9-30-2017
        self.fy_end_date = date(int(fy), 9, 30)

    # Creates a filter object using date field, will return a Q object such that
    # Q(date_field__gte=start_date) & Q(date_field__lte=end_date)
    def get_filter_object(self, date_field, as_dict=False):
        date_start = {}
        date_end = {}
        date_start[date_field + '__gte'] = self.fy_start_date
        date_end[date_field + '__lte'] = self.fy_end_date
        if as_dict:
            return {**date_start, **date_end}
        else:
            return (Q(**date_start) & Q(**date_end))


# This class supports multiple methods of dynamically creating filter queries
class FilterGenerator():
    operators = {
        # Django standard operations
        'equals': '',
        'less_than': '__lt',
        'greater_than': '__gt',
        'contains': '__icontains',
        'in': '__in',
        'less_than_or_equal': '__lte',
        'greather_than_or_equal': '__gte',
        'range': '__range',
        'is_null': '__isnull',

        # Special operations follow
        'fy': 'fy'
    }

    # Creating the class requires a filter map - this maps one parameter filter
    # key to another, for instance you could map "fpds_code" to "subtier_agency__fpds_code"
    # This is useful for allowing users to filter on a fk relationship without
    # having to specify the more complicated filter
    # Additionally, ignored parameters specifies parameters to ignore. Always includes
    # to ["page", "limit"]
    def __init__(self, filter_map={}, ignored_parameters=[]):
        self.filter_map = filter_map
        self.ignored_parameters = ['page', 'limit'] + ignored_parameters

    # Pass in request.GET and you'll get back a **kwargs object suitable for use
    # in .filter()
    # NOTE: GET will really only support 'AND' of filters, to use OR we'll need
    # a more complex request object via POST
    def create_from_get(self, parameters):
        return_arguments = {}
        for key in parameters:
            if key in self.ignored_parameters:
                continue
            if key in self.filter_map:
                return_arguments[self.filter_map[key]] = parameters[key]
            else:
                return_arguments[key] = parameters[key]
        return return_arguments

    # Creates a Q object from a POST query. Example of a post query:
    # {
    #     'page': 1,
    #     'limit': 100,
    #     'filters': [
    #         {
    #             'combine_method': 'OR',
    #             'filters': [ . . . ]
    #         },
    #         {
    #             'field': <FIELD_NAME>
    #             'operation': <OPERATION>
    #             'value': <VALUE>
    #         },
    #     ]
    # }
    # If the 'combine_method' is present in a filter, you MUST specify another
    # 'filters' set in that object of filters to combine
    # The combination method for filters at the root level is 'AND'
    # Available operations are equals, less_than, greater_than, contains, in, less_than_or_equal, greather_than_or_equal, range, fy
    # Note that contains is always case insensitive
    def create_from_post(self, parameters):
        try:
            self.validate_post_request(parameters)
        except Exception:
            raise
        return self.create_q_from_filter_list(parameters.get('filters', []))

    def create_q_from_filter_list(self, filter_list, combine_method='AND'):
        q_object = Q()
        for filt in filter_list:
            if combine_method is 'AND':
                q_object &= self.create_q_from_filter(filt)
            elif combine_method is 'OR':
                q_object |= self.create_q_from_filter(filt)
        return q_object

    def create_q_from_filter(self, filt):
        if 'combine_method' in filt:
            return self.create_q_from_filter_list(filt['filters'], filt['combine_method'])
        else:
            q_kwargs = {}
            field = filt['field']
            operation = FilterGenerator.operators[filt['operation']]
            value = filt['value']

            # It's unlikely anyone would specify and ignored parameter via post
            if field in self.ignored_parameters:
                return Q()
            if field in self.filter_map:
                field = self.filter_map[field]

            if operation is 'fy':
                fy = FiscalYear(value)
                return fy.get_filter_object(field)

            q_kwargs[field + operation] = value

            return Q(**q_kwargs)

    def validate_post_request(self, request):
        if 'filters' in request:
            for filt in request['filters']:
                if 'combine_method' in filt:
                    try:
                        self.validate_post_request(filt)
                    except Exception:
                        raise
                else:
                    if 'field' in filt and 'operation' in filt and 'value' in filt:
                        if filt['operation'] not in FilterGenerator.operators:
                            raise Exception("Invalid operation: " + filt['operation'])
                        if filt['operation'] == 'in':
                            if not isinstance(filt['value'], list):
                                raise Exception("Invalid value, operation 'in' requires an array value")
                        if filt['operation'] == 'range':
                            if not isinstance(filt['value'], list) or len(filt['value']) != 2:
                                raise Exception("Invalid value, operation 'range' requires an array value of length 2")
                    else:
                        raise Exception("Malformed filter - missing field, operation, or value")


class ResponsePaginator():
    @staticmethod
    def get_paged_data(data_set, page=1, page_limit=100, request_parameters={}):
        if 'limit' in request_parameters:
            page_limit = int(request_parameters['limit'])
        if 'page' in request_parameters:
            page = request_parameters['page']

        paginator = Paginator(data_set, page_limit)

        try:
            paged_data = paginator.page(page)
        except PageNotAnInteger:
            # Either no page or garbage page
            paged_data = paginator.page(1)
            page = 1
        except EmptyPage:
            # Page is too far, give last page
            paged_data = paginator.page(paginator.num_pages)
            page = paginator.num_pages

        return paged_data
