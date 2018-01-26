CREATE EXTENSION postgres_fdw;
CREATE EXTENSION dblink;

CREATE SERVER <server name>
        FOREIGN DATA WRAPPER postgres_fdw
        OPTIONS (host '<host>', port '<port>', dbname '<database name>');

-- Use `select inet_server_addr();` on broker database to get private IP

CREATE USER MAPPING FOR root
        SERVER <server name>
        OPTIONS (user '<username>', password '<password>');

SELECT * FROM dblink('<server name>', '<query>') as <table name> (<column name> <data type>);


DROP SCHEMA IF EXISTS broker CASCADE;


CREATE SCHEMA broker;


IMPORT FOREIGN SCHEMA public
   LIMIT TO (published_award_financial_assistance)
   FROM SERVER <server name>
   INTO broker;