-- Nuke and pave the server, with the requisite USER MAPPING
-- REQUIRES:
-- - postgres_fdw EXTENSION in ../extensions/extensions.sql

-- RUNNING:
-- 1) Export environment variables
-- 2) Substitute in-place using Bash eval (example: https://stackoverflow.com/a/12422379)

-- DROPS
DROP USER MAPPING IF EXISTS FOR usaspending SERVER broker_server;
DROP SERVER IF EXISTS broker_server;

-- CREATES
CREATE SERVER broker_server FOREIGN DATA WRAPPER postgres_fdw OPTIONS (host 'localhost', dbname '', port '5432');
CREATE USER MAPPING FOR usaspending SERVER broker_server OPTIONS (user 'usaspending', password 'usaspender');
