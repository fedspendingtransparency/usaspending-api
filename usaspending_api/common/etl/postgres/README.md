# ETL Helpers

One of the things we want to do more often is perform in-place creates, updates, and
deletes for destination tables during ETL processes.  This is a very common practice and
one that typically requires a lot of boilerplate code to be written, especially when
dealing with tables comprised of dozens or hundreds of columns.

The goal of this set of helper tools is to attempt to cut back on the amount of
boilerplate a bit by automating column, key, and data type detection and automating SQL
generation.

## Objects

- ETLTable - Represents a table in the database.  Really just abstracts away much of
the database introspection bits and encapsulates table properties to reduce function
call interfaces.
- ETLTemporaryTable - Same as ETLTable, but designed to work with temporary tables.
- ETLDBLinkTable - An ETLTable that lives on another server accessible via a dblink
connection.  Read only.
- ETLQuery - Treats a query like a table.  Read only.
- ETLQueryFile - Exactly the same as ETLQuery except reads the query from a file.

## Operations

- delete_obsolete_rows -  Delete rows from destination that do not exist in source and
return the number of rows deleted.
- identify_new_or_updated - Create a temporary staging table containing keys of rows in
source that are new or updated from destination and return the number of rows affected.
- insert_missing_rows - Insert rows from source that do not exist in destination and return
the number of rows inserted.
- stage_table - Copy source table contents to local staging table and return
the number of rows copied.
- update_changed_rows - Update rows in destination that have changed in source and return
the number of rows updated.

# Examples

Taken from actual code, this snippet will synchronize a local `broker_subaward` table
with the contents of a remote, dblinked `subaward` table.
```
from usaspending_api.common.etl.postgres import ETLTable, operations


# Establish some ETL tables.
broker_subaward = ETLTable(table_name="broker_subaward", schema_name="public")
remote_subaward = ETLDBLinkTable(table_name="subaward", schema_name="public", dblink_name="broker_server")
temp_broker_subaward = ETLTemporaryTable(table_name="temp_load_subawards_broker_subaward")
temp_new_or_updated = ETLTemporaryTable(table_name="temp_load_subawards_new_or_updated")

# Copy Broker's subaward table to a local staging table.
operations.stage_table(source=remote_subaward, destination=broker_subaward, staging=temp_broker_subaward)

# Not actually material to this particular synchronization example, but this is how you
# create a list of new or updated subawards that we can use to filter down subsequent
# operations.
operations.identify_new_or_updated(source=temp_broker_subaward, destination=broker_subaward, staging=temp_new_or_updated)

# Perform the actual synchronization.
operations.delete_obsolete_rows(source=temp_broker_subaward, destination=broker_subaward)
operations.update_changed_rows(source=temp_broker_subaward, destination=broker_subaward)
operations.insert_missing_rows(source=temp_broker_subaward, destination=broker_subaward)
```
Our local `broker_subaward` table should now match the Broker's `subaward` table for
columns we maintain on this side of the divide.

# Conclusion

The current implementation is a bit simplistic.  It handles only columns that exist in
both tables.  Future implementations will want to handle:

- Column mappings for situations where source and destination have differing columns.
- A proper ETLColumn class to replace the current ColumnDefinition named tuple.
- A shared database connection.  Currently we are relying on there only being one
  writable database connection in the app.
- and so much more... there's so much we can do with this
