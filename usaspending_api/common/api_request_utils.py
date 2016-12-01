from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
from django.contrib.postgres.search import SearchVector
from django.db.models import Q, Count
from django.utils import timezone
from datetime import date, time, datetime


# This class represents a fiscal year
# Currently it functions only on the basis of federal fiscal years, more rules
# may be added for other jurisdictions
class FiscalYear():
    def __init__(self, fy):
        self.fy = fy
        tz = time(0, 0, 1, tzinfo=timezone.utc)
        # FY start previous year on Oct 1st. i.e. FY 2017 starts 10-1-2016
        self.fy_start_date = datetime.combine(date(int(fy)-1, 10, 1), tz)
        # FY ends current FY year on Sept 30th i.e. FY 2017 ends 9-30-2017
        self.fy_end_date = datetime.combine(date(int(fy), 9, 30), tz)

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
        'greater_than_or_equal': '__gte',
        'range': '__range',
        'is_null': '__isnull',
        'search': '__search',

        # Special operations follow
        'fy': 'fy',
        'range_intersect': 'range_intersect'
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
        # When using full-text search the surrounding code must check for search vectors!
        self.search_vectors = []

    # Attaches this generator's search vectors to the query_set, if there are any
    def attach_search_vectors(self, query_set):
        qs = query_set
        if len(self.search_vectors) > 0:
            vector_sum = self.search_vectors[0]
            for vector in self.search_vectors[1:]:
                vector_sum += vector
            qs.annotate(search=vector_sum)
        return qs

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
            if combine_method == 'AND':
                q_object &= self.create_q_from_filter(filt)
            elif combine_method == 'OR':
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

            value_format = None
            if 'value_format' in filt:
                value_format = filt['value_format']

            # Special multi-field case for full-text search
            if isinstance(field, list) and operation is '__search':
                # We create the search vector and attach it to this object
                sv = SearchVector(*field)
                self.search_vectors.append(sv)
                # Our Q object is simpler now
                q_kwargs['search'] = value
                # Return our Q and skip the rest
                return Q(**q_kwargs)

            # Handle special operations
            if operation is 'fy':
                fy = FiscalYear(value)
                return fy.get_filter_object(field)
            if operation is 'range_intersect':
                # If we have a value_format and it is fy, convert it to the
                # date range for that fiscal year
                if value_format and value_format == 'fy':
                    fy = FiscalYear(value)
                    value = [fy.fy_start_date, fy.fy_end_date]
                return self.range_intersect(field, value)

            # We don't have a special operation, so handle the remaining cases
            # It's unlikely anyone would specify and ignored parameter via post
            if field in self.ignored_parameters:
                return Q()
            if field in self.filter_map:
                field = self.filter_map[field]

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
                        if filt['operation'] == 'range_intersect':
                            if not isinstance(filt['field'], list) or len(filt['field']) != 2:
                                raise Exception("Invalid field, operation 'range_intersect' requires an array of length 2 for field")
                            if (not isinstance(filt['value'], list) or len(filt['value']) != 2) and 'value_format' not in filt:
                                raise Exception("Invalid value, operation 'range_intersect' requires an array value of length 2, or a single value with value_format set to a ranged format (such as fy)")
                    else:
                        raise Exception("Malformed filter - missing field, operation, or value")

    # Special operation functions follow

    # Range intersect function - evaluates if a range defined by two fields overlaps
    # a range of values
    # Here's a picture:
    #                 f1 - - - f2
    #                       r1 - - - r2     - Case 1
    #             r1 - - - r2               - Case 2
    #                 r1 - - - r2           - Case 3
    # All of the ranges defined by [r1,r2] intersect [f1,f2]
    # i.e. f1 <= r2 && r1 <= f2 we intersect!
    # Returns: Q object to perform this operation
    # Parameters - Make sure these are in order:
    #           fields - A list defining the fields forming the first range (in order)
    #           values - A list of the values which define the second range (in order)
    def range_intersect(self, fields, values):
        # Create the Q filter case
        q_case = {}
        q_case[fields[0] + "__lte"] = values[1]  # f1 <= r2
        q_case[fields[1] + "__gte"] = values[0]  # f2 >= r1
        return Q(**q_case)


# Handles unique value requests
class UniqueValueHandler():
    @staticmethod
    # Data set to use (should be filtered already)
    # Fields to find unique values for
    # Returns a dictionary containing fields, values and their counts
    # {
    #   "recipient__name": {
    #       "Jon": 5,
    #       "Joe": 2
    #   }
    # }
    def get_values_and_counts(data_set, fields):
        data_set = data_set.all()  # Do this because we don't want to get finnicky with annotations
        response_object = {}
        if fields:
            for field in fields:
                response_object[field] = {}
                unique_values = data_set.values(field).distinct()
                for value in unique_values:
                    q_kwargs = {}
                    q_kwargs[field] = value[field]
                    print(q_kwargs)
                    response_object[field][value[field]] = data_set.filter(**q_kwargs).count()
        return response_object


# Handles autocomplete requests
class AutoCompleteHandler():
    @staticmethod
    # Data set to be searched for the value, and which fields to look in
    # Mode is either "contains" or "startswith"
    def get_values_and_counts(data_set, fields, value, mode="contains"):
        value_dict = {}
        count_dict = {}

        if mode == "contains":
            mode = "__icontains"
        elif mode == "startswith":
            mode = "__istartswith"

        for field in fields:
            q_args = {}
            q_args[field + mode] = value
            value_dict[field] = list(set(data_set.filter(Q(**q_args)).values_list(field, flat=True)))  # Why this weirdness? To ensure we eliminate duplicates
            count_dict[field] = len(value_dict[field])

        return value_dict, count_dict

    @staticmethod
    def handle(data_set, body):
        try:
            AutoCompleteHandler.validate(body)
        except:
            raise
        if "mode" not in body:
            body["mode"] = "contains"
        value_dict, count_dict = AutoCompleteHandler.get_values_and_counts(data_set, body["fields"], body["value"], body["mode"])
        return {
            "counts": count_dict,
            "results": value_dict
        }

    @staticmethod
    def validate(body):
        if "fields" in body and "value" in body:
            if not isinstance(body["fields"], list):
                raise Exception("Invalid field, autocomplete fields value must be a list")
        else:
            raise Exception("Invalid request, autocomplete requests need parameters 'fields' and 'value'")
        if "mode" in body:
            if body["mode"] not in ["contains", "startswith"]:
                raise Exception("Invalid mode, autocomplete modes are 'contains', 'startswith', but got " + body["mode"])


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
