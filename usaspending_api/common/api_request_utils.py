from collections import OrderedDict
from datetime import date, time, datetime

from django.core.paginator import Paginator, EmptyPage, PageNotAnInteger
from django.contrib.postgres.search import SearchVector
from django.db.models import Q
from django.utils import timezone

from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.references.models import Location
from usaspending_api.awards.models import Award


class FiscalYear():
    """Represents a federal fiscal year."""
    def __init__(self, fy):
        self.fy = fy
        tz = time(0, 0, 1, tzinfo=timezone.utc)
        # FY start previous year on Oct 1st. i.e. FY 2017 starts 10-1-2016
        self.fy_start_date = datetime.combine(date(int(fy) - 1, 10, 1), tz)
        # FY ends current FY year on Sept 30th i.e. FY 2017 ends 9-30-2017
        self.fy_end_date = datetime.combine(date(int(fy), 9, 30), tz)

    """
    Creates a filter object using date field, will return a Q object such that
    Q(date_field__gte=start_date) & Q(date_field__lte=end_date)
    """
    def get_filter_object(self, date_field, as_dict=False):
        """
        Create a filter object using date field, will return a Q object
        such that Q(date_field__gte=start_date) & Q(date_field__lte=end_date)
        """
        date_start = {}
        date_end = {}
        date_start[date_field + '__gte'] = self.fy_start_date
        date_end[date_field + '__lte'] = self.fy_end_date
        if as_dict:
            return {**date_start, **date_end}
        else:
            return (Q(**date_start) & Q(**date_end))


class FilterGenerator():
    """Support for multiple methods of dynamically creating filter queries."""
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

    """
    Creating the class requires a filter map - this maps one parameter filter
    key to another, for instance you could map "fpds_code" to "subtier_agency__fpds_code"
    This is useful for allowing users to filter on a fk relationship without
    having to specify the more complicated filter
    Additionally, ignored parameters specifies parameters to ignore. Always includes
    to ["page", "limit"]
    """
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

    """
    Pass in request.GET and you'll get back a **kwargs object suitable for use
    in .filter()
    NOTE: GET will really only support 'AND' of filters, to use OR we'll need
    a more complex request object via POST
    """
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

    """
    Creates a Q object from a POST query. Example of a post query:
    {
        'page': 1,
        'limit': 100,
        'filters': [
            {
                'combine_method': 'OR',
                'filters': [ . . . ]
            },
            {
                'field': <FIELD_NAME>
                'operation': <OPERATION>
                'value': <VALUE>
            },
        ]
    }
    If the 'combine_method' is present in a filter, you MUST specify another
    'filters' set in that object of filters to combine
    The combination method for filters at the root level is 'AND'
    Available operations are equals, less_than, greater_than, contains, in, less_than_or_equal, greather_than_or_equal, range, fy
    Note that contains is always case insensitive
    """
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
            negate = False
            if "not_" == filt['operation'][:4]:
                negate = True
                operation = FilterGenerator.operators[filt['operation'][4:]]
            else:
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
                if negate:
                    return ~Q(**q_kwargs)
                return Q(**q_kwargs)

            # Handle special operations
            if operation is 'fy':
                fy = FiscalYear(value)
                if negate:
                    return ~fy.get_filter_object(field)
                return fy.get_filter_object(field)
            if operation is 'range_intersect':
                # If we have a value_format and it is fy, convert it to the
                # date range for that fiscal year
                if value_format and value_format == 'fy':
                    fy = FiscalYear(value)
                    value = [fy.fy_start_date, fy.fy_end_date]
                if negate:
                    return ~self.range_intersect(field, value)
                return self.range_intersect(field, value)

            # We don't have a special operation, so handle the remaining cases
            # It's unlikely anyone would specify and ignored parameter via post
            if field in self.ignored_parameters:
                return Q()
            if field in self.filter_map:
                field = self.filter_map[field]

            q_kwargs[field + operation] = value

            if negate:
                return ~Q(**q_kwargs)
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
                        if filt['operation'] not in FilterGenerator.operators and filt['operation'][:4] is not 'not_' and filt['operation'][4:] not in FilterGenerator.operators:
                            raise InvalidParameterException("Invalid operation: " + filt['operation'])
                        if filt['operation'] == 'in':
                            if not isinstance(filt['value'], list):
                                raise InvalidParameterException("Invalid value, operation 'in' requires an array value")
                        if filt['operation'] == 'range':
                            if not isinstance(filt['value'], list) or len(filt['value']) != 2:
                                raise InvalidParameterException("Invalid value, operation 'range' requires an array value of length 2")
                        if filt['operation'] == 'range_intersect':
                            if not isinstance(filt['field'], list) or len(filt['field']) != 2:
                                raise InvalidParameterException("Invalid field, operation 'range_intersect' requires an array of length 2 for field")
                            if (not isinstance(filt['value'], list) or len(filt['value']) != 2) and 'value_format' not in filt:
                                raise InvalidParameterException("Invalid value, operation 'range_intersect' requires an array value of length 2, or a single value with value_format set to a ranged format (such as fy)")
                    else:
                        raise InvalidParameterException("Malformed filter - missing field, operation, or value")

    # Special operation functions follow

    """
    Range intersect function - evaluates if a range defined by two fields overlaps
    a range of values
    Here's a picture:
                    f1 - - - f2
                          r1 - - - r2     - Case 1
                r1 - - - r2               - Case 2
                    r1 - - - r2           - Case 3
    All of the ranges defined by [r1,r2] intersect [f1,f2]
    i.e. f1 <= r2 && r1 <= f2 we intersect!
    Returns: Q object to perform this operation
    Parameters - Make sure these are in order:
              fields - A list defining the fields forming the first range (in order)
              values - A list of the values which define the second range (in order)
    """
    def range_intersect(self, fields, values):
        # Create the Q filter case
        q_case = {}
        q_case[fields[0] + "__lte"] = values[1]  # f1 <= r2
        q_case[fields[1] + "__gte"] = values[0]  # f2 >= r1
        return Q(**q_case)


# Handles unique value requests
class UniqueValueHandler:

    @staticmethod
    def get_values_and_counts(data_set, fields):
        """Get unique values for specified fields in a filtered queryset.

        Keyword arguments:
            data_set -- Django QuerySet
            fields -- list of fields in data_set that we unique values for

        Returns:
            A dictionary keyed by fields. Each entry is another dictionary that
            contains the field's unique values and corresponding counts.
            For example:
            {
              "recipient__name": {
                  "Jon": 5,
                  "Joe": 2
              }
            }

        """
        data_set = data_set.all()  # Do this because we don't want to get finicky with annotations
        response_object = {}
        if fields:
            for field in fields:
                response_object[field] = {}
                unique_values = data_set.values(field).distinct()
                for value in unique_values:
                    q_kwargs = {field: value[field]}
                    response_object[field][value[field]] = data_set.filter(**q_kwargs).count()
        return response_object


# Handles autocomplete requests
class AutoCompleteHandler():
    @staticmethod
    # Data set to be searched for the value, and which ids to match
    def get_values_and_counts(data_set, filter_matched_ids, pk_name):
        value_dict = {}
        count_dict = {}

        for field in filter_matched_ids.keys():
            q_args = {pk_name + "__in": filter_matched_ids[field]}
            value_dict[field] = list(set(data_set.all().filter(Q(**q_args)).values_list(field, flat=True)))  # Why this weirdness? To ensure we eliminate duplicates
            count_dict[field] = len(value_dict[field])

        return value_dict, count_dict

    '''
    Returns an array of ids that match the filters for the given fields
    '''
    @staticmethod
    def get_filter_matched_ids(data_set, fields, value, mode="contains", limit=10):
        if mode == "contains":
            mode = "__icontains"
        elif mode == "startswith":
            mode = "__istartswith"

        filter_matched_ids = {}
        pk_name = data_set.model._meta.pk.name
        for field in fields:
            q_args = {}
            q_args[field + mode] = value
            filter_matched_ids[field] = data_set.all().filter(Q(**q_args)).select_related(field)[:limit].values_list(pk_name, flat=True)

        return filter_matched_ids, pk_name

    @staticmethod
    def get_objects(data_set, filter_matched_ids, pk_name, serializer):
        matched_objects = {}

        for field in filter_matched_ids.keys():
            q_args = {}
            q_args[pk_name + "__in"] = filter_matched_ids[field]
            matched_object_qs = data_set.all().filter(Q(**q_args))
            matched_objects[field] = serializer(matched_object_qs, many=True).data

        return matched_objects

    @staticmethod
    def handle(data_set, body, serializer=None):
        try:
            AutoCompleteHandler.validate(body)
        except:
            raise

        return_object = {}

        filter_matched_ids, pk_name = AutoCompleteHandler.get_filter_matched_ids(data_set.all(), body["fields"], body["value"], body.get("mode", "contains"), body.get("limit", 10))

        # Get matching string values, and their counts
        value_dict, count_dict = AutoCompleteHandler.get_values_and_counts(data_set.all(), filter_matched_ids, pk_name)

        # Get the matching objects, if requested
        if body.get("matched_objects", False) and serializer:
            return_object["matched_objects"] = AutoCompleteHandler.get_objects(data_set.all(), filter_matched_ids, pk_name, serializer)

        return {
            **return_object,
            "counts": count_dict,
            "results": value_dict,
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


class GeoCompleteHandler:
    """ Handles geographical hierarchy searches """

    def __init__(self, request_body):
        self.request_body = request_body
        self.search_fields = OrderedDict()
        self.search_fields["location_country_code__country_name"] = {
            "type": "COUNTRY",
            "parent": "location_country_code"
        }
        self.search_fields["location_state_code"] = {
            "type": "STATE",
            "parent": "location_country_code__country_name"
        }
        self.search_fields["location_state_name"] = {
            "type": "STATE",
            "parent": "location_country_code__country_name"
        }
        self.search_fields["location_city_name"] = {
            "type": "CITY",
            "parent": "location_state_name"
        }
        self.search_fields["location_county_name"] = {
            "type": "COUNTY",
            "parent": "location_state_name"
        }
        self.search_fields["location_zip5"] = {
            "type": "ZIP",
            "parent": "location_state_name"
        }
        self.search_fields["location_foreign_postal_code"] = {
            "type": "POSTAL CODE",
            "parent": "location_country_code__country_name"
        }
        self.search_fields["location_foreign_province"] = {
            "type": "PROVINCE",
            "parent": "location_country_code__country_name"
        }
        self.search_fields["location_foreign_city_name"] = {
            "type": "CITY",
            "parent": "location_country_code__country_name"
        }

    def build_response(self):
        # Array of search fields, and their 'parent' fields and types
        search_fields = self.search_fields
        value = self.request_body.get("value", None)
        mode = self.request_body.get("mode", "contains")
        scope = self.request_body.get("scope", "all")
        usage = self.request_body.get("usage", "all")
        limit = self.request_body.get("limit", 10)

        if mode == "contains":
            mode = "__icontains"
        elif mode == "startswith":
            mode = "__istartswith"

        scope_q = Q()
        if scope == "foreign":
            scope_q = ~Q(**{"location_country_code": "USA"})
        elif scope == "domestic":
            scope_q = Q(**{"location_country_code": "USA"})

        usage_q = Q()
        if usage == "recipient":
            usage_q = Q(recipient_flag=True)
        elif usage == "place_of_performance":
            usage_q = Q(place_of_performance_flag=True)

        response_object = []

        """
        The front end will send congressional codes as XX-## format, where XX
        the state code (location_state_code) and ## is the two digit district
        code. (location_congressional_code). If we find a '-' in the value,
        attempt to parse it as a congressional code search before the others
        """
        if value and '-' in value:
            temp_val = value.split('-')

            q_kwargs = {}
            q_kwargs["location_state_code"] = temp_val[0]

            if len(temp_val) >= 2:
                q_kwargs["location_congressional_code__istartswith"] = temp_val[1]

            search_q = Q(**q_kwargs)
            results = Location.objects.filter(search_q & scope_q & usage_q).order_by("location_state_code", "location_congressional_code").values_list("location_congressional_code", "location_state_code", "location_state_name").distinct()[:limit]
            for row in results:
                response_row = {
                    "place": row[1] + "-" + str(row[0]),
                    "place_type": "CONGRESSIONAL DISTRICT",
                    "parent": row[2],
                    "matched_ids": Location.objects.filter(Q(**{"location_congressional_code": row[0], "location_state_code": row[1], "location_state_name": row[2]})).values_list("location_id", flat=True)
                }
                response_object.append(response_row)
                if len(response_object) >= limit:
                    return response_object

        if value:
            for searchable_field in search_fields.keys():
                search_q = Q(**{searchable_field + mode: value})
                results = Location.objects.filter(search_q & scope_q & usage_q).order_by(searchable_field).values_list(searchable_field, search_fields[searchable_field]["parent"]).distinct()[:limit]
                print(results)
                for row in results:
                    response_row = {
                        "place": row[0],
                        "place_type": search_fields[searchable_field]["type"],
                        "parent": row[1],
                        "matched_ids": Location.objects.filter(Q(**{searchable_field: row[0], search_fields[searchable_field]["parent"]: row[1]})).values_list("location_id", flat=True)
                    }
                    response_object.append(response_row)
                    if len(response_object) >= limit:
                        return response_object

        return response_object


class DataQueryHandler:
    """Handles complex queries via POST requests data."""

    def __init__(self, model, serializer, request_body, agg_list=[], ordering=None):
        self.request_body = request_body
        self.serializer = serializer
        self.model = model
        self.agg_list = agg_list
        self.ordering = ordering

    def build_response(self):
        """Returns a dictionary from a POST request that can be used to create a response."""
        fg = FilterGenerator()
        filters = fg.create_from_post(self.request_body)
        # Grab the ordering
        self.ordering = self.request_body.get("order", self.ordering)

        records = self.model.objects.all()

        if len(fg.search_vectors) > 0:
            vector_sum = fg.search_vectors[0]
            for vector in fg.search_vectors[1:]:
                vector_sum += vector
            records = records.annotate(search=vector_sum)

        # filter model records
        records = records.filter(filters)

        # Order the response
        if self.ordering:
            records = records.order_by(*self.ordering)

        # if this request specifies unique values, get those
        unique_values = UniqueValueHandler.get_values_and_counts(
            records, self.request_body.get('unique_values', None))

        # construct metadata of entire set of data that matches the request specifications
        metadata = {"count": records.count()}
        # for each aggregate field/function passed in, calculate value and add to metadata
        aggregates = {
            '{}_{}'.format(a.field, a.func.__name__.lower()):
                next(iter(records.aggregate(a.func(a.field)).values())) for a in self.agg_list}
        metadata.update(aggregates)

        # get paged data for this request
        paged_data = ResponsePaginator.get_paged_data(
            records, request_parameters=self.request_body)
        paged_queryset = paged_data.object_list.all()

        # construct page-specific metadata
        page_metadata = {
            "page_number": paged_data.number,
            "num_pages": paged_data.paginator.num_pages,
            "count": len(paged_data)
        }
        page_aggregates = {
            '{}_{}'.format(a.field, a.func.__name__.lower()):
                next(iter(paged_queryset.aggregate(a.func(a.field)).values())) for a in self.agg_list}
        page_metadata.update(page_aggregates)

        # serialize the paged data
        fields = self.request_body.get('fields', None)
        exclude = self.request_body.get('exclude', None)
        serializer = self.serializer(paged_data, fields=fields, exclude=exclude, many=True)
        serialized_data = serializer.data

        response_object = OrderedDict({
            "unique_values_metadata": unique_values,
            "total_metadata": metadata,
            "page_metadata": page_metadata
        })
        response_object.update({'results': serialized_data})

        return response_object


class ResponsePaginator:
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
