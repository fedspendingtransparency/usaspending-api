# Init DB

These are a series of SQL scripts that are used to configure the Docker created Postgres database.

## create_users.sql

Creates both the `etl_user` and `readonly` user that appear across the different functions of our application and the database download.

## set_search_path.sql

By default the `usaspending` user is only able to see the `public` schema. When performing a `pg_restore` of the database download this can cause a problem with visibility into the different database schemas. Normally, if the `usaspending` user created the different tables it would have access, but in the case of the database download that is not the case. To get around this we update the `search_path` of the `usaspending` user to include the different schemas in the `data_store_api` database.
