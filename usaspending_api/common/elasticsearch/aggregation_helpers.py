from elasticsearch_dsl import A


def create_count_aggregation(field_name):
    """
    This method creates an ElasticSearch aggregation that determines the count of unique
    entries for a provided field name.
    """
    return A(
        "scripted_metric",
        params={"fieldName": field_name},
        init_script="state.list = []",
        map_script="if(doc[params.fieldName].size() > 0) state.list.add(doc[params.fieldName].value);",
        combine_script="return state.list;",
        reduce_script="""
            Set uniqueValues = new HashSet();
            for (shardList in states) {
                if (shardList != null) {
                    Set tempValues = new HashSet(shardList);
                    uniqueValues.addAll(tempValues);
                }
            }
            return uniqueValues.size();
        """,
    )
