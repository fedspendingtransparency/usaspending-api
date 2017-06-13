from rest_framework import serializers


class BudgetFunctionAutocompleteSerializer(serializers.Serializer):

    budget_function_title = serializers.ListField()
    budget_subfunction_title = serializers.ListField()
