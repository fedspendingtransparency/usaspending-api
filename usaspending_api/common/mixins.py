import logging
from typing import Any

from django.core.exceptions import FieldDoesNotExist
from django.db import models
from django.db.models import Avg, Count, ExpressionWrapper, F, Func, IntegerField, Max, Min, Model, Q, QuerySet, Sum
from django.db.models.fields import Field
from django.db.models.functions import ExtractDay, ExtractMonth, ExtractYear
from django.utils import timezone
from pgvector.django import VectorField
from rest_framework.request import Request

from usaspending_api.common.api_request_utils import AutoCompleteHandler, FilterGenerator
from usaspending_api.common.exceptions import InvalidParameterException
from usaspending_api.llm.embeddings.embedding_generator import EmbeddingGenerator

logger = logging.getLogger(__name__)


class AggregateQuerysetMixin(object):
    """
    Aggregate a queryset.
    Any pre-aggregation operations on the queryset (e.g. filtering) is already done (it's handled at the view level,
    in the get_queryset method).
    """

    def aggregate(self, request: Request, *args, **kwargs) -> QuerySet:
        """Perform an aggregate function on a Django queryset with an optional group by field."""
        # create a single dict that contains the requested aggregate parameters, regardless of request type
        # (e.g., GET, POST) (not sure if this is a good practice, or we should be more prescriptive that aggregate
        # requests can only be of one type)
        params = dict(request.query_params)
        params.update(dict(request.data))

        # get the queryset to be aggregated
        queryset = kwargs.get("queryset", None)

        # validate request parameters
        agg_field, group_fields, date_part = self.validate_request(params, queryset)

        # Check for null opt-in, and filter instances where all group fields are null
        if not params.get("show_null_groups", False) and not params.get("show_nulls", False):
            q_object = Q()
            for field in group_fields:
                q_object = q_object | Q(**{"{}__isnull".format(field): False})
            queryset = queryset.filter(q_object)

        # Check for null opt-in, and filter instances where the aggregate field is null
        if not params.get("show_null_aggregates", False) and not params.get("show_nulls", False):
            q_object = Q(**{"{}__isnull".format(agg_field): False})
            queryset = queryset.filter(q_object)

        # get the aggregate function to use (default is Sum)
        agg_map = {"avg": Avg, "count": Count, "min": Min, "max": Max, "sum": Sum}
        agg_function = params.get("aggregate", "sum").lower()
        agg_function = agg_map.get(agg_function, Sum)

        if group_fields and date_part:
            # group queryset by a date field and aggregate
            group_func_map = {"year": ExtractYear, "month": ExtractMonth, "day": ExtractDay}
            group_func = group_func_map.get(date_part)
            aggregate = (
                queryset.annotate(item=group_func(group_fields[0]))
                .values("item")
                .annotate(aggregate=agg_function(agg_field))
            )
        else:
            # item is deprecated and should be removed soon group queryset by a non-date field and aggregate

            # Support expression wrappers on all items in the group field array
            # We must do this so users can specify a __fy request on any field
            # in any order, rather than being required to do so as the first item
            item_annotations = {"item": self._wrapped_f_expression(group_fields[0])}
            for gf in group_fields:
                expr = self._wrapped_f_expression(gf)
                if isinstance(expr, ExpressionWrapper):
                    item_annotations[gf] = expr
            group_fields.append("item")
            aggregate = (
                queryset.annotate(**item_annotations).values(*group_fields).annotate(aggregate=agg_function(agg_field))
            )

        return aggregate

    _sql_function_transformations = {"fy": IntegerField}

    def _resolve_field_path(self, model: type[Model], field_path: str) -> tuple[str, Field | None]:
        segments = field_path.split("__")
        current_model = model
        resolved_field = None

        for index, segment in enumerate(segments):
            try:
                resolved_field = current_model._meta.get_field(segment)
            except FieldDoesNotExist:
                # If this is the last segment and it's a known transform, allow it.
                is_last = index == len(segments) - 1
                if is_last and segment in self._sql_function_transformations:
                    # Valid transform suffix, no need to continue validation.
                    return field_path, None
                raise InvalidParameterException(f"Invalid field: {field_path}") from None

            is_last = index == len(segments) - 1
            if resolved_field.is_relation and not is_last:
                current_model = resolved_field.related_model
            elif not is_last:
                # Check if the next segment is a valid SQL function transformation
                next_segment = segments[index + 1]
                is_next_last = (index + 1) == len(segments) - 1
                if is_next_last and next_segment in self._sql_function_transformations:
                    # Valid transform suffix on this field, allow it
                    return field_path, None
                raise InvalidParameterException(f"Invalid field: {field_path}")

        return field_path, resolved_field

    def _validate_field_path(self, model: type[Model], field_path: str) -> str:
        self._resolve_field_path(model, field_path)
        return field_path

    def _wrapped_f_expression(self, col_name: str) -> F | ExpressionWrapper:
        """F-expression of col, wrapped if needed with SQL function call

        Assumes that there's an SQL function defined for each registered lookup.
        """
        for suffix in self._sql_function_transformations:
            full_suffix = "__" + suffix
            if col_name.endswith(full_suffix):
                col_name = col_name[: -(len(full_suffix))]
                result = Func(F(col_name), function=suffix)
                output_type = self._sql_function_transformations[suffix]
                result = ExpressionWrapper(result, output_field=output_type())
                return result
        return F(col_name)

    def validate_request(self, params: dict[str, Any], queryset: QuerySet) -> tuple[str, list[str], str | None]:
        """Validate request parameters."""

        agg_field = params.get("field")
        group_fields = params.get("group")
        date_part = params.get("date_part")
        model = queryset.model

        # field to aggregate is required
        if agg_field is None:
            raise InvalidParameterException("Request is missing the name of the field to aggregate")

        # make sure the field we're aggregating exists in the model
        if hasattr(model, agg_field) is False:
            raise InvalidParameterException(
                "Field {} not found in model {}. Please specify a valid field in the request.".format(agg_field, model)
            )

        # make sure the field we're aggregating on is numeric
        # (there is likely a better way to do this?)
        numeric_fields = [
            "BigIntegerField",
            "DecimalField",
            "FloatField",
            "IntegerField",
            "PositiveIntegerField",
            "PositiveSmallIntegerField",
            "SmallIntegerField",
            "DurationField",
        ]
        if model._meta.get_field(agg_field).get_internal_type() not in numeric_fields:
            raise InvalidParameterException(
                "Aggregate field {} is not a numeric type (e.g., integer, decimal)".format(agg_field)
            )

        # field to group by is required
        if group_fields is None:
            raise InvalidParameterException("Request is missing the field to group by")

        # make sure group fields is a list
        if not isinstance(group_fields, list):
            group_fields = [group_fields]

        for group_field in group_fields:
            self._validate_field_path(model, group_field)

        # if a groupby date part is specified, make sure the groupby field is
        # a date and the groupby value is year, quarter, or month
        if date_part is not None:
            # only allow date parts when grouping by a single field (for now)
            if len(group_fields) > 1:
                raise InvalidParameterException("Date parts are only valid when grouping by a single field.")
            # if the request is asking to group by a date component, the field
            # we're grouping by must be a date-related field (there is probably a better way to do this?)
            date_fields = ["DateField", "DateTimeField"]
            _, group_field = self._resolve_field_path(model, group_fields[0])
            if group_field.get_internal_type() not in date_fields:
                raise InvalidParameterException(
                    "Group by date part ({}) requested for a non-date group by ({})".format(date_part, group_fields[0])
                )
            # date_part must be a supported date component
            supported_date_parts = ["year", "month", "quarter", "day"]
            date_part = date_part.lower()
            if date_part not in supported_date_parts:
                raise InvalidParameterException(
                    "Date part {} is unsupported. Supported date parts are {}".format(date_part, supported_date_parts)
                )

        return agg_field, group_fields, date_part


class FilterQuerysetMixin(object):
    """Handles queryset filtering."""

    def filter_records(self, request: Request, *args, **kwargs) -> QuerySet:
        """Filter a queryset based on request parameters"""
        queryset = kwargs.get("queryset")

        # If there is data in the request body, use that to create filters. Otherwise, use information in the request's
        # query params to create filters. Eventually, we should refactor the filter creation process to accept a list
        # of parameters and create filters without needing to know about the structure of the request itself.
        filters = None
        filter_map = kwargs.get("filter_map", {})
        fg = FilterGenerator(queryset.model, filter_map=filter_map)

        if len(request.data):
            fg = FilterGenerator(queryset.model)
            filters = fg.create_from_request_body(request.data)
        else:
            filters = Q(**fg.create_from_query_params(request.query_params))

        # Handle FTS vectors
        if len(fg.search_vectors) > 0:
            vector_sum = fg.search_vectors[0]
            for vector in fg.search_vectors[1:]:
                vector_sum += vector
            queryset = queryset.annotate(search=vector_sum)

        subwhere = filters
        # Create structure the query so we don't need to use distinct
        # This happens by reforming the request as 'WHERE pk_id IN (SELECT pk_id FROM queryset WHERE filters)'
        if len(filters) > 0:
            subwhere = Q(
                **{
                    queryset.model._meta.pk.name + "__in": queryset.filter(filters).values_list(
                        queryset.model._meta.pk.name, flat=True
                    )
                }
            )

        return queryset.filter(subwhere)

    def order_records(self, request: Request, *args, **kwargs) -> QuerySet:
        """Order a queryset based on request parameters."""
        queryset = kwargs.get("queryset")

        # create a single dict that contains the requested aggregate parameters,
        # regardless of request type (e.g., GET, POST)
        # (not sure if this is a good practice, or we should be more
        # prescriptive that aggregate requests can only be of one type)

        params = dict(request.query_params)
        params.update(dict(request.data))
        ordering = params.get("order")
        if ordering is not None:
            return queryset.order_by(*ordering)
        else:
            return queryset

    def get_submission_id_filters(self) -> tuple[int | None, list[int]]:
        """
        Returns the federal_account_id and the list of fiscal_years from the list of incoming
        filters if they exist. If not, return None and an empty list respectively
        """
        federal_account_id = None
        fiscal_years = []
        if "filters" in self.request.data:
            for filter in self.request.data["filters"]:
                if filter["field"] == "treasury_account__federal_account_id":
                    federal_account_id = filter["value"]
                if filter["field"] == "submission__reporting_fiscal_year":
                    fiscal_years = filter["value"]
        return federal_account_id, fiscal_years


class AutocompleteResponseMixin(object):
    """Handles autocomplete responses and requests"""

    def build_response(self, request: Request, *args, **kwargs) -> Any:
        queryset = kwargs.get("queryset")

        serializer = kwargs.get("serializer")

        params = request.query_params.copy()  # copy() creates mutable copy of a QueryDict
        params.update(request.data.copy())

        return AutoCompleteHandler.handle(queryset, params, serializer)


class EmbeddingMixin(models.Model):
    embedding_dimensions: int = 256
    embedding: VectorField = VectorField(dimensions=embedding_dimensions, null=True, blank=True)
    embedding_generated_at: models.DateTimeField = models.DateTimeField(null=True, blank=True)

    class Meta:
        abstract = True

    def get_embedding_text(self) -> str | None:
        raise NotImplementedError(f"{self.__class__.__name__} must implement get_embedding_text()")

    def get_embedding_generator(self) -> EmbeddingGenerator:
        return EmbeddingGenerator(dimensions=self.embedding_dimensions)

    def generate_embedding(self, force: bool = False, verbose: bool = False) -> bool:
        did_generate = False
        if self.embedding is not None and not force:
            logger.debug(f"Embedding already exists for: {self.__class__.__name__} {self.pk}")
        else:
            text = self.get_embedding_text()

            if not text or not text.strip():
                logger.warning(f"Embedding text is empty for: {self.__class__.__name__} {self.pk}")
            else:
                try:
                    generator = self.get_embedding_generator()
                    self.embedding = generator.generate_embedding(text)
                    self.embedding_generated_at = timezone.now()
                    if verbose:
                        logger.info(f"Generated embedding for {self.__class__.__name__} {self.pk}")
                    did_generate = True
                except Exception as e:
                    logger.exception(f"Failed to generate embedding for {self.__class__.__name__} {self.pk}: {e}")
        return did_generate

    @property
    def has_embedding(self) -> bool:
        return self.embedding is not None

    def save(
        self,
        auto_generate_embedding: bool = True,
        force_generate_embedding: bool = False,
        verbose: bool = False,
        *args,
        **kwargs,
    ) -> None:
        if auto_generate_embedding and not self.has_embedding or force_generate_embedding:
            try:
                self.generate_embedding(verbose=verbose, force=force_generate_embedding)
            except Exception as e:
                logger.error(
                    f"Failed to auto-generate embedding during save for {self.__class__.__name__} {self.pk}: {e}"
                )
        super().save(*args, **kwargs)
