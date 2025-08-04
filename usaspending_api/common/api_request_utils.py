from datetime import date, time, datetime
from django.contrib.postgres.search import SearchVector
from django.db.models import Q
from django.utils import timezone
from usaspending_api.common.exceptions import InvalidParameterException


class FiscalYear:
    """Represents a federal fiscal year."""

    def __init__(self, fy):
        self.fy = fy
        tz = time(0, 0, 1, tzinfo=timezone.utc)
        # FY start previous year on Oct 1st. i.e. FY 2017 starts 10-1-2016
        self.fy_start_date = datetime.combine(date(int(fy) - 1, 10, 1), tz)
        # FY ends current FY year on Sept 30th i.e. FY 2017 ends 9-30-2017
        self.fy_end_date = datetime.combine(date(int(fy), 9, 30), tz)

    def get_filter_object(self, date_field, as_dict=False):
        """
        Create a filter object using date field, will return a Q object
        such that Q(date_field__gte=start_date) & Q(date_field__lte=end_date)
        """
        date_start = {}
        date_end = {}
        date_start[date_field + "__gte"] = self.fy_start_date
        date_end[date_field + "__lte"] = self.fy_end_date
        if as_dict:
            return {**date_start, **date_end}
        else:
            return Q(**date_start) & Q(**date_end)


class FilterGenerator:
    """
    Creating the class requires a filter map - this maps one parameter filter
    key to another, for instance you could map "subtier_code" to "subtier_agency__subtier_code"
    This is useful for allowing users to filter on a fk relationship without
    having to specify the more complicated filter
    Additionally, ignored parameters specifies parameters to ignore. Always includes
    to ["page", "limit", "last"]
    """

    # Support for multiple methods of dynamically creating filter queries.
    operators = {
        # Django standard operations
        "equals": "",
        "less_than": "__lt",
        "greater_than": "__gt",
        "contains": "__icontains",
        "less_than_or_equal": "__lte",
        "greater_than_or_equal": "__gte",
        "range": "__range",
        "is_null": "__isnull",
        "search": "__search",
        # ArrayField operations
        "overlap": "__overlap",
        "contained_by": "__contained_by",
        "length_greater_than": "__len__gt",
        "length_less_than": "__len__lt",
        # Special operations follow
        "in": "in",
        "fy": "fy",
        "range_intersect": "range_intersect",
    }

    def __init__(self, model, filter_map={}, ignored_parameters=[]):
        self.filter_map = filter_map
        self.model = model
        self.ignored_parameters = ["page", "limit", "last", "req", "verbose"] + ignored_parameters
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

    # We should refactor create_from_query_params
    # and create_from_request_body into a single
    # method that can create filters based on passed-in
    # parameters without needing to know about the structure
    # of the request itself (e.g., GET vs POST)
    def create_from_query_params(self, parameters):
        """
        Create filters using a request's query parameters.

        NOTE: GET only supports 'AND' filters. Anything more complex
        will need to be specified in the body of a POST request.

        Returns:
            A **kwargs object suitable for use in .filter()
        """
        return_arguments = {}
        for key in parameters:
            if key in self.ignored_parameters:
                continue
            if key in self.filter_map:
                return_arguments[self.filter_map[key]] = parameters[key]
            else:
                return_arguments[key] = parameters[key]
        return return_arguments

    def create_from_request_body(self, parameters):
        """
        Creates a Q object from a POST query.

        Example of a post query:
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

        If the 'combine_method' is present in a filter, you MUST specify another 'filters' set in that object of filters
        to combine
        The combination method for filters at the root level is 'AND'
        Available operations are equals, less_than, greater_than, contains, in, less_than_or_equal,
        greather_than_or_equal, range, fy
        Note that contains is always case insensitive
        """
        try:
            self.validate_post_request(parameters)
        except Exception:
            raise
        return self.create_q_from_filter_list(parameters.get("filters", []))

    def create_q_from_filter_list(self, filter_list, combine_method="AND"):
        q_object = Q()
        for filt in filter_list:
            if combine_method == "AND":
                q_object &= self.create_q_from_filter(filt)
            elif combine_method == "OR":
                q_object |= self.create_q_from_filter(filt)
        return q_object

    def create_q_from_filter(self, filt):
        if "combine_method" in filt:
            return self.create_q_from_filter_list(filt["filters"], filt["combine_method"])
        else:
            q_kwargs = {}
            field = filt["field"]
            negate = False
            if "not_" == filt["operation"][:4]:
                negate = True
                operation = FilterGenerator.operators[filt["operation"][4:]]
            else:
                operation = FilterGenerator.operators[filt["operation"]]
            value = filt["value"]

            value_format = None
            if "value_format" in filt:
                value_format = filt["value_format"]

            # Special multi-field case for full-text search
            if isinstance(field, list) and operation == "__search":
                # We create the search vector and attach it to this object
                sv = SearchVector(*field)
                self.search_vectors.append(sv)
                # Our Q object is simpler now
                q_kwargs["search"] = value
                # Return our Q and skip the rest
                if negate:
                    return ~Q(**q_kwargs)
                return Q(**q_kwargs)

            # Handle special operations
            if operation == "fy":
                fy = FiscalYear(value)
                if negate:
                    return ~fy.get_filter_object(field)
                return fy.get_filter_object(field)
            if operation == "range_intersect":
                # If we have a value_format and it is fy, convert it to the
                # date range for that fiscal year
                if value_format and value_format == "fy":
                    fy = FiscalYear(value)
                    value = [fy.fy_start_date, fy.fy_end_date]
                if negate:
                    return ~self.range_intersect(field, value)
                return self.range_intersect(field, value)
            if operation == "in":
                # make in operation case insensitive for string fields
                if self.is_string_field(field):
                    q_obj = Q()
                    for item in value:
                        new_q = {}
                        new_q[field + "__iexact"] = item
                        new_q = Q(**new_q)
                        q_obj = q_obj | new_q
                    if negate:
                        q_obj = ~q_obj
                    return q_obj
                else:
                    # Otherwise, use built in django in
                    operation = "__in"
            if operation == "__icontains" and isinstance(value, list):
                # In cases where we have a list of contains (e.g. ArrayField searches)
                # we need to not do this case insensitive, as ArrayField's don't have
                # icontains implemented like contains
                operation = "__contains"
            if operation == "" and self.is_string_field(field):
                # If we're doing a simple comparison, we need to use iexact for
                # string fields
                operation = "__iexact"

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
        if "filters" in request:
            for filt in request["filters"]:
                if "combine_method" in filt:
                    try:
                        self.validate_post_request(filt)
                    except Exception:
                        raise
                else:
                    if "field" in filt and "operation" in filt and "value" in filt:
                        if (
                            filt["operation"] not in FilterGenerator.operators
                            and filt["operation"][:4] != "not_"
                            and filt["operation"][4:] not in FilterGenerator.operators
                        ):
                            raise InvalidParameterException("Invalid operation: " + filt["operation"])
                        if filt["operation"] == "in":
                            if not isinstance(filt["value"], list):
                                raise InvalidParameterException("Invalid value, operation 'in' requires an array value")
                        if filt["operation"] == "range":
                            if not isinstance(filt["value"], list) or len(filt["value"]) != 2:
                                raise InvalidParameterException(
                                    "Invalid value, operation 'range' requires an array value of length 2"
                                )
                        if filt["operation"] == "range_intersect":
                            if not isinstance(filt["field"], list) or len(filt["field"]) != 2:
                                raise InvalidParameterException(
                                    "Invalid field, operation 'range_intersect' "
                                    "requires an array of length 2 for field"
                                )
                            if (
                                not isinstance(filt["value"], list) or len(filt["value"]) != 2
                            ) and "value_format" not in filt:
                                raise InvalidParameterException(
                                    "Invalid value, operation 'range_intersect' requires "
                                    "an array value of length 2, or a single value with "
                                    "value_format set to a ranged format (such as fy)"
                                )
                        if filt["operation"] in ["overlap", "contained_by"] and not isinstance(filt["value"], list):
                            raise InvalidParameterException(
                                "Invalid value. When using operation {}, value must be an "
                                "array of strings.".format(filt["operation"])
                            )
                        if filt["operation"] == "search":
                            if not isinstance(filt["field"], list) and not self.is_string_field(filt["field"]):
                                raise InvalidParameterException(
                                    "Invalid field: '"
                                    + filt["field"]
                                    + "', operation 'search' requires a text-field for "
                                    "searching"
                                )
                            elif isinstance(filt["field"], list):
                                for search_field in filt["field"]:
                                    if not self.is_string_field(search_field):
                                        raise InvalidParameterException(
                                            "Invalid field: '"
                                            + search_field
                                            + "', operation 'search' requires a text-field "
                                            "for searching"
                                        )
                    else:
                        raise InvalidParameterException("Malformed filter - missing field, operation, or value")

    # Special operation functions follow

    def range_intersect(self, fields, values):
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

        # Create the Q filter case
        q_case = {}
        q_case[fields[0] + "__lte"] = values[1]  # f1 <= r2
        q_case[fields[1] + "__gte"] = values[0]  # f2 >= r1
        return Q(**q_case)

    def is_string_field(self, field):
        fields = field.split("__")
        model_to_check = self.model

        # If fields > 1, we're following a fk traversal - we need to move the model we're checking
        # down via the fk path, then check the field on that model
        if len(fields) > 1:
            while len(fields) > 1:
                mf = model_to_check._meta.get_field(fields.pop(0))
                # Check if this field is a foreign key
                if mf.get_internal_type() in ["ForeignKey", "ManyToManyField", "OneToOneField"]:
                    # Continue traversal
                    related = getattr(mf, "remote_field", None)
                    if related:
                        model_to_check = related.model
                    else:
                        model_to_check = mf.related_model
                else:
                    # We've hit something that ISN'T a related field, which means it is either
                    # a lookup, or a field with '__' in the name. In either case, we can return
                    # false here
                    return False
        return model_to_check._meta.get_field(fields[0]).get_internal_type() in ["TextField", "CharField"]


# Handles autocomplete requests
class AutoCompleteHandler:
    @staticmethod
    # Data set to be searched for the value, and which ids to match
    def get_values_and_counts(data_set, filter_matched_ids, pk_name):
        value_dict = {}
        count_dict = {}

        for field in filter_matched_ids.keys():
            q_args = {pk_name + "__in": filter_matched_ids[field]}
            # Why this weirdness? To ensure we eliminate duplicates
            value_dict[field] = list(set(data_set.all().filter(Q(**q_args)).values_list(field, flat=True)))
            count_dict[field] = len(value_dict[field])

        return value_dict, count_dict

    """
    Returns an array of ids that match the filters for the given fields
    """

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
            filter_matched_ids[field] = data_set.all().filter(Q(**q_args))[:limit].values_list(pk_name, flat=True)

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
        except Exception:
            raise

        # If the serializer supports eager loading, set it up
        if serializer:
            if hasattr(serializer, "setup_eager_loading") and callable(serializer.setup_eager_loading):
                data_set = serializer.setup_eager_loading(data_set)

        return_object = {}

        filter_matched_ids, pk_name = AutoCompleteHandler.get_filter_matched_ids(
            data_set.all(), body["fields"], body["value"], body.get("mode", "contains"), body.get("limit", 10)
        )

        # Get matching string values, and their counts
        value_dict, count_dict = AutoCompleteHandler.get_values_and_counts(data_set.all(), filter_matched_ids, pk_name)

        # Get the matching objects, if requested
        if body.get("matched_objects", False) and serializer:
            return_object["matched_objects"] = AutoCompleteHandler.get_objects(
                data_set.all(), filter_matched_ids, pk_name, serializer
            )

        return {**return_object, "counts": count_dict, "results": value_dict}

    @staticmethod
    def validate(body):
        if "fields" in body and "value" in body:
            if not isinstance(body["fields"], list):
                raise InvalidParameterException("Invalid field, autocomplete fields value must be a list")
        else:
            raise InvalidParameterException(
                "Invalid request, autocomplete requests need parameters 'fields' and 'value'"
            )
        if "mode" in body:
            if body["mode"] not in ["contains", "startswith"]:
                raise InvalidParameterException(
                    "Invalid mode, autocomplete modes are 'contains', 'startswith', but got " + body["mode"]
                )
