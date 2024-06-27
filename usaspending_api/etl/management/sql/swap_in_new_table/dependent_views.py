detect_dep_view_sql = """
    SELECT
        dependent_ns.nspname AS dep_view_schema,
        dependent_view.relname AS dep_view_name,
        CONCAT(dependent_ns.nspname, '.', dependent_view.relname) as dep_view_fullname,
        RTRIM(pg_get_viewdef(CONCAT(dependent_ns.nspname, '.', dependent_view.relname), TRUE), ';') AS dep_view_sql,
        dependent_view.relkind = 'm' AS is_matview
    FROM pg_depend
    JOIN pg_rewrite ON pg_depend.objid = pg_rewrite.oid
    JOIN pg_class as dependent_view ON pg_rewrite.ev_class = dependent_view.oid
    JOIN pg_class as source_table ON pg_depend.refobjid = source_table.oid
    JOIN pg_attribute ON pg_depend.refobjid = pg_attribute.attrelid
        AND pg_depend.refobjsubid = pg_attribute.attnum
    JOIN pg_namespace dependent_ns ON dependent_ns.oid = dependent_view.relnamespace
    JOIN pg_namespace source_ns ON source_ns.oid = source_table.relnamespace
    WHERE
        source_ns.nspname = '{curr_schema_name}'
        AND source_table.relname = '{curr_table_name}'
    GROUP BY
        CONCAT(source_ns.nspname, '.', source_table.relname),
        dependent_ns.nspname,
        dependent_view.relname,
        CONCAT(dependent_ns.nspname, '.', dependent_view.relname),
        dependent_view.relkind;
"""
