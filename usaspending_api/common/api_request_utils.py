import json
import os
from datetime import date, datetime, time
from functools import wraps
from typing import Any, Dict, List, Optional, Union

import boto3
from botocore.exceptions import ClientError
from django.contrib.postgres.search import SearchVector
from django.db.models import Q
from django.utils import timezone
from rest_framework import status
from rest_framework.response import Response

from usaspending_api.common.exceptions import InvalidParameterException


class FiscalYear:
    """Represents a federal fiscal year."""

    def __init__(self, fy: int) -> None:
        self.fy = fy
        tz = time(0, 0, 1, tzinfo=timezone.utc)
        # FY start previous year on Oct 1st. i.e. FY 2017 starts 10-1-2016
        self.fy_start_date = datetime.combine(date(int(fy) - 1, 10, 1), tz)
        # FY ends current FY year on Sept 30th i.e. FY 2017 ends 9-30-2017
        self.fy_end_date = datetime.combine(date(int(fy), 9, 30), tz)

    def get_filter_object(self, date_field: str, as_dict: bool = False) -> Union[Q, Dict[str, Any]]:
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

    def __init__(
        self,
        model: Any,
        filter_map: Optional[Dict[str, str]] = None,
        ignored_parameters: Optional[List[str]] = None,
    ) -> None:
        self.filter_map = filter_map if filter_map is not None else {}
        self.model = model
        base_ignored = ["page", "limit", "last", "req", "verbose"]
        self.ignored_parameters = base_ignored + (ignored_parameters if ignored_parameters is not None else [])
        # When using full-text search the surrounding code must check for search vectors!
        self.search_vectors = []

    # Attaches this generator's search vectors to the query_set, if there are any
    def attach_search_vectors(self, query_set: Any) -> Any:
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
    def create_from_query_params(self, parameters: Dict[str, Any]) -> Dict[str, Any]:
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

    def create_from_request_body(self, parameters: Dict[str, Any]) -> Q:
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

    def create_q_from_filter_list(self, filter_list: List[Dict[str, Any]], combine_method: str = "AND") -> Q:
        q_object = Q()
        for filt in filter_list:
            if combine_method == "AND":
                q_object &= self.create_q_from_filter(filt)
            elif combine_method == "OR":
                q_object |= self.create_q_from_filter(filt)
        return q_object

    def create_q_from_filter(self, filt: Dict[str, Any]) -> Q:  # noqa: C901, PLR0911, PLR0912, PLR0915
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

    def validate_post_request(self, request: Dict[str, Any]) -> None:
        if "filters" in request:
            for filt in request["filters"]:
                self._validate_single_filter(filt)

    def _validate_single_filter(self, filt: Dict[str, Any]) -> None:
        """Validate a single filter object"""
        if "combine_method" in filt:
            self.validate_post_request(filt)
        elif "field" in filt and "operation" in filt and "value" in filt:
            self._validate_filter_operation(filt)
        else:
            raise InvalidParameterException("Malformed filter - missing field, operation, or value")

    def _validate_filter_operation(self, filt: Dict[str, Any]) -> None:
        """Validate the operation and value for a filter"""
        operation = filt["operation"]

        # Validate operation exists
        if not self._is_valid_operation(operation):
            raise InvalidParameterException("Invalid operation: " + operation)

        # Validate operation-specific requirements
        if operation == "in":
            self._validate_in_operation(filt)
        elif operation == "range":
            self._validate_range_operation(filt)
        elif operation == "range_intersect":
            self._validate_range_intersect_operation(filt)
        elif operation in ["overlap", "contained_by"]:
            self._validate_array_operation(filt)
        elif operation == "search":
            self._validate_search_operation(filt)

    def _is_valid_operation(self, operation: str) -> bool:
        """Check if operation is valid (including negated operations)"""
        if operation in FilterGenerator.operators:
            return True
        if operation[:4] == "not_" and operation[4:] in FilterGenerator.operators:
            return True
        return False

    def _validate_in_operation(self, filt: Dict[str, Any]) -> None:
        """Validate 'in' operation requires array value"""
        if not isinstance(filt["value"], list):
            raise InvalidParameterException("Invalid value, operation 'in' requires an array value")

    def _validate_range_operation(self, filt: Dict[str, Any]) -> None:
        """Validate 'range' operation requires array of length 2"""
        if not isinstance(filt["value"], list) or len(filt["value"]) != 2:
            raise InvalidParameterException(
                "Invalid value, operation 'range' requires an array value of length 2"
            )

    def _validate_range_intersect_operation(self, filt: Dict[str, Any]) -> None:
        """Validate 'range_intersect' operation requirements"""
        if not isinstance(filt["field"], list) or len(filt["field"]) != 2:
            raise InvalidParameterException(
                "Invalid field, operation 'range_intersect' "
                "requires an array of length 2 for field"
            )

        has_valid_value = isinstance(filt["value"], list) and len(filt["value"]) == 2
        has_value_format = "value_format" in filt

        if not has_valid_value and not has_value_format:
            raise InvalidParameterException(
                "Invalid value, operation 'range_intersect' requires "
                "an array value of length 2, or a single value with "
                "value_format set to a ranged format (such as fy)"
            )

    def _validate_array_operation(self, filt: Dict[str, Any]) -> None:
        """Validate 'overlap' and 'contained_by' operations require array values"""
        if not isinstance(filt["value"], list):
            raise InvalidParameterException(
                "Invalid value. When using operation {}, value must be an "
                "array of strings.".format(filt["operation"])
            )

    def _validate_search_operation(self, filt: Dict[str, Any]) -> None:
        """Validate 'search' operation requires text fields"""
        field = filt["field"]

        if isinstance(field, list):
            self._validate_search_field_list(field)
        elif not self.is_string_field(field):
            raise InvalidParameterException(
                "Invalid field: '" + field +
                "', operation 'search' requires a text-field for searching"
            )

    def _validate_search_field_list(self, field_list: List[str]) -> None:
        """Validate all fields in a search operation are text fields"""
        for search_field in field_list:
            if not self.is_string_field(search_field):
                raise InvalidParameterException(
                    "Invalid field: '" + search_field +
                    "', operation 'search' requires a text-field for searching"
                )

    # Special operation functions follow

    def range_intersect(self, fields: List[str], values: List[Any]) -> Q:
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

    def is_string_field(self, field: str) -> bool:
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
    def get_values_and_counts(data_set: Any, filter_matched_ids: Dict[str, Any], pk_name: str) -> tuple:
        value_dict = {}
        count_dict = {}

        for field in filter_matched_ids.keys():
            q_args = {pk_name + "__in": filter_matched_ids[field]}
            # Why this weirdness? To ensure we eliminate duplicates
            value_dict[field] = list(
                set(data_set.all().filter(Q(**q_args)).values_list(field, flat=True)))
            count_dict[field] = len(value_dict[field])

        return value_dict, count_dict

    """
    Returns an array of ids that match the filters for the given fields
    """

    @staticmethod
    def get_filter_matched_ids(
            data_set: Any, fields: List[str], value: str, mode: str = "contains", limit: int = 10
    ) -> tuple:
        if mode == "contains":
            mode = "__icontains"
        elif mode == "startswith":
            mode = "__istartswith"

        filter_matched_ids = {}
        pk_name = data_set.model._meta.pk.name
        for field in fields:
            q_args = {}
            q_args[field + mode] = value
            filter_matched_ids[field] = data_set.all().filter(Q(**q_args))[:limit].values_list(pk_name,
                                                                                               flat=True)

        return filter_matched_ids, pk_name

    @staticmethod
    def get_objects(data_set: Any, filter_matched_ids: Dict[str, Any], pk_name: str, serializer: Any) -> \
    Dict[str, Any]:
        matched_objects = {}

        for field in filter_matched_ids.keys():
            q_args = {}
            q_args[pk_name + "__in"] = filter_matched_ids[field]
            matched_object_qs = data_set.all().filter(Q(**q_args))
            matched_objects[field] = serializer(matched_object_qs, many=True).data

        return matched_objects

    @staticmethod
    def handle(data_set: Any, body: Dict[str, Any], serializer: Any = None) -> Dict[str, Any]:
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
            data_set.all(), body["fields"], body["value"], body.get("mode", "contains"),
            body.get("limit", 10)
        )

        # Get matching string values, and their counts
        value_dict, count_dict = AutoCompleteHandler.get_values_and_counts(data_set.all(),
                                                                           filter_matched_ids, pk_name)

        # Get the matching objects, if requested
        if body.get("matched_objects", False) and serializer:
            return_object["matched_objects"] = AutoCompleteHandler.get_objects(
                data_set.all(), filter_matched_ids, pk_name, serializer
            )

        return {**return_object, "counts": count_dict, "results": value_dict}

    @staticmethod
    def validate(body: Dict[str, Any]) -> None:
        if "fields" in body and "value" in body:
            if not isinstance(body["fields"], list):
                raise InvalidParameterException(
                    "Invalid field, autocomplete fields value must be a list")
        else:
            raise InvalidParameterException(
                "Invalid request, autocomplete requests need parameters 'fields' and 'value'"
            )
        if "mode" in body:
            if body["mode"] not in ["contains", "startswith"]:
                raise InvalidParameterException(
                    "Invalid mode, autocomplete modes are 'contains', 'startswith', but got " + body[
                        "mode"]
                )


class LLMAPIKeyHandler:
    """Handles LLM API key validation via AWS Secrets Manager"""

    @staticmethod
    def require_api_key(function: Any) -> Any:
        """
        Decorator to validate LLM API access via UUID in request header.
        Checks against UUID stored in AWS Secrets Manager.

        Must be applied before cache decorators to prevent unauthorized cache access.

        Environment Variables:
            LLM_API_SECRET_NAME: Name of the AWS secret containing the LLM API UUID
            AWS_REGION: AWS region for Secrets Manager (defaults to us-gov-west-1)

        Request Headers:
            X-LLM-API-Key: UUID for LLM API authentication

        Returns 403 Forbidden with appropriate message if:
            - X-LLM-API-Key header is not included in the API request
            - AWS doesn't contain the requested secret
            - UUID in the header doesn't match the AWS secret

        Example:
            @LLMAPIKeyHandler.require_api_key
            @cache_response()
            @api_view(['GET', 'POST'])
            def llm_endpoint(request):
                return Response({"message": "LLM endpoint"})
        """

        @wraps(function)
        def wrapper(*args: Any, **kwargs: Any) -> Response:
            # Extract request from args
            request = args[0] if args else kwargs.get('request')

            # Validate request and authentication
            error_response = LLMAPIKeyHandler._validate_llm_request(request)
            if error_response:
                return error_response

            # UUID is valid, proceed with the view
            return function(*args, **kwargs)

        return wrapper

    @staticmethod
    def _validate_llm_request(request: Any) -> Optional[Response]:
        """
        Validate LLM API request and authenticate via AWS Secrets Manager.
        Returns error Response if validation fails, None if successful.
        """
        if not request:
            return LLMAPIKeyHandler._error_response(
                "Request object not found",
                status.HTTP_500_INTERNAL_SERVER_ERROR
            )

        llm_api_key = request.headers.get('X-LLM-API-Key')
        if not llm_api_key:
            return LLMAPIKeyHandler._error_response(
                "X-LLM-API-Key header is required for LLM API access",
                status.HTTP_403_FORBIDDEN
            )

        secret_name = os.environ.get('LLM_API_SECRET_NAME')
        if not secret_name:
            return LLMAPIKeyHandler._error_response(
                "LLM API secret configuration is not set",
                status.HTTP_403_FORBIDDEN
            )

        # Retrieve and validate secret from AWS
        stored_uuid = LLMAPIKeyHandler._get_secret_uuid(secret_name)
        if isinstance(stored_uuid, Response):
            return stored_uuid

        # Validate UUID matches
        if llm_api_key.strip() != stored_uuid.strip():
            return LLMAPIKeyHandler._error_response(
                "Invalid LLM API key",
                status.HTTP_403_FORBIDDEN
            )

        return None

    @staticmethod
    def _get_secret_uuid(secret_name: str) -> Union[str, Response]:
        """
        Retrieve UUID from AWS Secrets Manager.
        Returns UUID string on success or error Response on failure.
        """
        try:
            session = boto3.session.Session()
            client = session.client(
                service_name='secretsmanager',
                region_name=os.environ.get('AWS_REGION', 'us-gov-west-1')
            )

            get_secret_value_response = client.get_secret_value(SecretId=secret_name)

            if 'SecretString' not in get_secret_value_response:
                return LLMAPIKeyHandler._error_response(
                    "LLM API secret format is invalid",
                    status.HTTP_403_FORBIDDEN
                )

            return LLMAPIKeyHandler._parse_secret_uuid(get_secret_value_response['SecretString'])

        except ClientError as e:
            return LLMAPIKeyHandler._handle_client_error(e, secret_name)
        except Exception as e:
            return LLMAPIKeyHandler._error_response(
                f"Unexpected error accessing LLM API secret: {str(e)}",
                status.HTTP_403_FORBIDDEN
            )

    @staticmethod
    def _parse_secret_uuid(secret: str) -> Union[str, Response]:
        """
        Parse UUID from secret string (handles both JSON and plain text).
        Returns UUID string or error Response.
        """
        try:
            secret_dict = json.loads(secret)
            stored_uuid = (
                secret_dict.get('uuid') or
                secret_dict.get('LLM_API_KEY') or
                secret_dict.get('api_key')
            )
            if not stored_uuid:
                return LLMAPIKeyHandler._error_response(
                    "LLM API secret does not contain a valid UUID key",
                    status.HTTP_403_FORBIDDEN
                )
            return stored_uuid
        except json.JSONDecodeError:
            # Secret is a plain string UUID
            return secret

    @staticmethod
    def _handle_client_error(error: ClientError, secret_name: str) -> Response:
        """Handle AWS ClientError and return appropriate error response."""
        error_code = error.response['Error']['Code']
        if error_code == 'ResourceNotFoundException':
            detail = f"LLM API secret '{secret_name}' not found in AWS Secrets Manager"
        else:
            detail = f"Error retrieving LLM API secret: {error_code}"

        return LLMAPIKeyHandler._error_response(detail, status.HTTP_403_FORBIDDEN)

    @staticmethod
    def _error_response(detail: str, status_code: int) -> Response:
        """Create a standardized error response."""
        return Response({"detail": detail}, status=status_code)