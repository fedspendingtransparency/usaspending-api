CREATE EXTENSION postgres_fdw;
CREATE EXTENSION dblink;

CREATE SERVER <server name>
        FOREIGN DATA WRAPPER postgres_fdw
        OPTIONS (host '<host>', port '<port>', dbname '<database name>');
        
CREATE USER MAPPING FOR root
        SERVER <server name>
        OPTIONS (user '<username>', password '<password>');
        
SELECT * FROM dblink('<server name>', '<query>') as <table name> (<column name> <data type>);
