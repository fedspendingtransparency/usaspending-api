CREATE EXTENSION IF NOT EXISTS postgres_fdw


DROP SERVER IF EXISTS broker_fdw_server CASCADE


CREATE SERVER broker_fdw_server
   FOREIGN DATA WRAPPER postgres_fdw
   OPTIONS (host %(HOST)s, port %(PORT)s, dbname %(NAME)s)


CREATE USER MAPPING FOR CURRENT_USER
    SERVER broker_fdw_server
    OPTIONS (user %(USER)s, password %(PASSWORD)s);


DROP SCHEMA IF EXISTS broker CASCADE


CREATE SCHEMA broker


IMPORT FOREIGN SCHEMA public
   LIMIT TO (submission, appropriation, award_procurement, award_financial_assistance)
   FROM SERVER broker_fdw_server
   INTO broker
