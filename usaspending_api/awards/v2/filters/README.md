### Filters

Currently, award filtering uses the award.py and transaction.py files located in this folder
Filtering is done by passing in a filters dictionary defined
[here](https://gist.github.com/nmonga91/ba0e172b6d3f2aaf50f0ef1bb5d708bc#agencies-awardingfunding-agency)


#### TODO: Dynamic Filtering
Currently filtering is static. In the future, we plan on abusing the filter(**kwargs) function to make a dynamic
dictionary of filters. EXAMPLE:

```
def dynamic_filter(queryset, filters, filter_keys):
```

This function will take a queryset, and will itereate through the filters.  If the filter is in the filter_keys,
it will grab the db mapping from filter_keys and dynamically make a filter based on the filters value.
EXAMPLE

filters = {"id": 1, "agency_name": "test"}
transaction_filter_keys{"id": "id", "award_name": "award__agecy__toptier_agency__name"}

dynamic_filter(Transactions.objects.all(), filters, transaction_filter_keys)

